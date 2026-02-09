defmodule Incoming.Session do
  @moduledoc false

  @behaviour :gen_smtp_server_session

  require Logger

  defstruct hostname: nil,
            peer: nil,
            queue: Incoming.Queue.Disk,
            queue_opts: [],
            max_message_size: 10 * 1024 * 1024,
            max_recipients: 100,
            tls_mode: :disabled,
            tls_active: false,
            mail_from: nil,
            rcpt_to: [],
            seen_helo: false

  @impl true
  def init(hostname, _session_count, peer, options) do
    {queue, queue_opts, max_message_size, max_recipients, tls_mode} = queue_from_opts(options)
    banner = [hostname, " ESMTP incoming"]

    state = %__MODULE__{
      hostname: hostname,
      peer: peer,
      queue: queue,
      queue_opts: queue_opts,
      max_message_size: max_message_size,
      max_recipients: max_recipients,
      tls_mode: tls_mode
    }

    case policy_check(:connect, state) do
      :ok ->
        emit(:connect, %{peer: peer, hostname: hostname})
        {:ok, banner, state}

      {:reject, code, message} ->
        emit(:rejected, %{reason: message})
        {:stop, :policy_reject, "#{code} #{message}"}
    end
  end

  @impl true
  def handle_HELO(_hostname, state) do
    case policy_check(:helo, state) do
      :ok -> {:ok, state.max_message_size, %{state | seen_helo: true}}
      {:reject, code, message} -> {:error, "#{code} #{message}", state}
    end
  end

  @impl true
  def handle_EHLO(_hostname, extensions, state) do
    case policy_check(:helo, state) do
      :ok ->
        exts =
          extensions
          |> size_extension(state.max_message_size)
          |> maybe_add_starttls(state)

        {:ok, exts, %{state | seen_helo: true}}

      {:reject, code, message} ->
        {:error, "#{code} #{message}", state}
    end
  end

  @impl true
  def handle_MAIL(from, state) do
    state = %{state | mail_from: from, rcpt_to: []}

    case policy_check(:mail_from, state) do
      :ok -> {:ok, state}
      {:reject, code, message} -> {:error, "#{code} #{message}", state}
    end
  end

  @impl true
  def handle_MAIL_extension(_extension, state) do
    {:ok, state}
  end

  @impl true
  def handle_RCPT(to, state) do
    if is_nil(state.mail_from) do
      {:error, resp(503, "Bad sequence of commands"), state}
    else
      state = %{state | rcpt_to: state.rcpt_to ++ [to]}

      if length(state.rcpt_to) > state.max_recipients do
        {:error, resp(452, "Too many recipients"), state}
      else
        case policy_check(:rcpt_to, state) do
          :ok -> {:ok, state}
          {:reject, code, message} -> {:error, resp(code, message), state}
        end
      end
    end
  end

  @impl true
  def handle_RCPT_extension(_extension, state) do
    {:ok, state}
  end

  @impl true
  def handle_DATA(from, to, data, state) do
    cond do
      is_nil(state.mail_from) ->
        {:error, resp(503, "Bad sequence of commands"), state}

      state.rcpt_to == [] ->
        {:error, resp(503, "Bad sequence of commands"), state}

      true ->
        do_handle_DATA(from, to, data, state)
    end
  end

  defp do_handle_DATA(from, to, data, state) do
    case policy_check(:data_start, state) do
      :ok ->
        if byte_size(data) > state.max_message_size do
          emit(:rejected, %{reason: :message_too_large})
          {:error, resp(552, "Message too large"), reset_envelope(state)}
        else
          enqueue_opts = Keyword.put(state.queue_opts, :max_message_size, state.max_message_size)

          result =
            if function_exported?(state.queue, :enqueue_stream, 4) do
              state.queue.enqueue_stream(from, to, [data], enqueue_opts)
            else
              state.queue.enqueue(from, to, data, enqueue_opts)
            end

          case result do
            {:ok, %Incoming.Message{id: id} = message} ->
              _ = policy_check(:message_complete, state)
              Incoming.Delivery.Dispatcher.dispatch(message)
              emit(:accepted, %{id: id})
              {:ok, "Ok: queued as <#{id}>", reset_envelope(state)}

            {:error, :message_too_large} ->
              emit(:rejected, %{reason: :message_too_large})
              {:error, resp(552, "Message too large"), reset_envelope(state)}

            {:error, reason} ->
              Logger.error("queue_error=#{inspect(reason)}")
              emit(:rejected, %{reason: reason})
              {:error, resp(451, "Temporary failure"), reset_envelope(state)}
          end
        end

      {:reject, code, message} ->
        emit(:rejected, %{reason: message})
        {:error, resp(code, message), reset_envelope(state)}
    end
  end

  defp reset_envelope(state) do
    %{state | mail_from: nil, rcpt_to: []}
  end

  @impl true
  def handle_RSET(state) do
    %{state | mail_from: nil, rcpt_to: []}
  end

  @impl true
  def handle_VRFY(_address, state) do
    {:error, resp(252, "VRFY disabled"), state}
  end

  @impl true
  def handle_other(_verb, _args, state) do
    {"500 Error: command not recognized", state}
  end

  @impl true
  def handle_STARTTLS(state) do
    %{state | tls_active: true}
  end

  @impl true
  def handle_info(_info, state) do
    {:noreply, state}
  end

  @impl true
  def handle_error(_class, _details, state) do
    {:ok, state}
  end

  @impl true
  def code_change(_old, state, _extra) do
    {:ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  defp queue_from_opts(options) do
    queue = Keyword.get(options, :queue, Incoming.Queue.Disk)
    queue_opts = Keyword.get(options, :queue_opts, [])
    max_message_size = Keyword.get(options, :max_message_size, 10 * 1024 * 1024)
    max_recipients = Keyword.get(options, :max_recipients, 100)
    tls_mode = Keyword.get(options, :tls_mode, :disabled)
    {queue, queue_opts, max_message_size, max_recipients, tls_mode}
  end

  defp policy_check(phase, state) do
    policies = Incoming.Config.policies()

    if policies == [] do
      :ok
    else
      Incoming.Policy.Pipeline.run(policies, %{
        phase: phase,
        peer: state.peer,
        hostname: state.hostname,
        envelope: %Incoming.Envelope{
          mail_from: state.mail_from,
          rcpt_to: state.rcpt_to
        },
        max_message_size: state.max_message_size,
        max_recipients: state.max_recipients,
        seen_helo: state.seen_helo,
        tls_mode: state.tls_mode,
        tls_active: state.tls_active
      })
    end
  end

  defp size_extension(extensions, max_size) do
    size = Integer.to_string(max_size) |> to_charlist()
    ext = Enum.reject(extensions, fn {key, _} -> to_string(key) == "SIZE" end)
    [{~c"SIZE", size} | ext]
  end

  defp maybe_add_starttls(extensions, %{tls_mode: mode, tls_active: false})
       when mode in [:optional, :required] do
    [{~c"STARTTLS", true} | extensions]
  end

  defp maybe_add_starttls(extensions, _state), do: extensions

  defp emit(event, meta) do
    Incoming.Metrics.emit([:incoming, :session, event], %{count: 1}, meta)
  end

  defp resp(code, message), do: "#{code} #{message}"
end
