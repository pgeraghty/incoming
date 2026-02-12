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
            max_commands: 1_000,
            max_errors: 10,
            max_connections_per_ip: 10,
            command_count: 0,
            error_count: 0,
            tls_mode: :disabled,
            tls_active: false,
            mail_from: nil,
            rcpt_to: [],
            seen_helo: false

  @impl true
  def init(hostname, _session_count, peer, options) do
    {queue, queue_opts, max_message_size, max_recipients, tls_mode, max_commands, max_errors,
     max_connections_per_ip} = session_from_opts(options)

    banner = [hostname, " ESMTP incoming"]

    state = %__MODULE__{
      hostname: hostname,
      peer: peer,
      queue: queue,
      queue_opts: queue_opts,
      max_message_size: max_message_size,
      max_recipients: max_recipients,
      max_commands: max_commands,
      max_errors: max_errors,
      max_connections_per_ip: max_connections_per_ip,
      tls_mode: tls_mode,
      tls_active: tls_mode == :implicit
    }

    case Incoming.ConnectionLimiter.inc(peer, state.max_connections_per_ip) do
      :ok ->
        case policy_check(:connect, state) do
          :ok ->
            emit(:connect, %{peer: peer, hostname: hostname})
            {:ok, banner, state}

          {:reject, code, message} ->
            Incoming.ConnectionLimiter.dec(peer)
            emit(:rejected, %{reason: message})
            {:stop, :policy_reject, resp(code, message)}
        end

      {:error, :limit_exceeded} ->
        emit(:rejected, %{reason: :too_many_connections})
        {:stop, :policy_reject, resp(421, "Too many connections")}
    end
  end

  @impl true
  def handle_HELO(_hostname, state) do
    state = inc_commands(state)

    if over_commands?(state) do
      state = schedule_stop(state)
      {:error, resp(421, "Too many commands"), state}
    else
      case policy_check(:helo, state) do
        :ok ->
          {:ok, state.max_message_size, %{state | seen_helo: true}}

        {:reject, code, message} ->
          {code, message, state} = inc_errors_and_maybe_limit(code, message, state)
          {:error, resp(code, message), state}
      end
    end
  end

  @impl true
  def handle_EHLO(_hostname, extensions, state) do
    state = inc_commands(state)

    if over_commands?(state) do
      state = schedule_stop(state)
      {:error, resp(421, "Too many commands"), state}
    else
      case policy_check(:helo, state) do
        :ok ->
          exts =
            extensions
            |> size_extension(state.max_message_size)
            |> maybe_add_starttls(state)

          {:ok, exts, %{state | seen_helo: true}}

        {:reject, code, message} ->
          {code, message, state} = inc_errors_and_maybe_limit(code, message, state)
          {:error, resp(code, message), state}
      end
    end
  end

  @impl true
  def handle_MAIL(from, state) do
    state = inc_commands(state)

    if over_commands?(state) do
      state = schedule_stop(state)
      {:error, resp(421, "Too many commands"), state}
    else
      candidate = %{state | mail_from: from, rcpt_to: []}

      case policy_check(:mail_from, candidate) do
        :ok ->
          {:ok, candidate}

        {:reject, code, message} ->
          {code, message, state} = inc_errors_and_maybe_limit(code, message, state)
          # Don't retain a rejected sender in the envelope.
          {:error, resp(code, message), %{state | mail_from: nil, rcpt_to: []}}
      end
    end
  end

  @impl true
  def handle_MAIL_extension(_extension, state) do
    {:ok, state}
  end

  @impl true
  def handle_RCPT(to, state) do
    state = inc_commands(state)

    cond do
      over_commands?(state) ->
        state = schedule_stop(state)
        {:error, resp(421, "Too many commands"), state}

      is_nil(state.mail_from) ->
        {code, message, state} =
          inc_errors_and_maybe_limit(503, "Bad sequence of commands", state)

        {:error, resp(code, message), state}

      length(state.rcpt_to) + 1 > state.max_recipients ->
        {code, message, state} = inc_errors_and_maybe_limit(452, "Too many recipients", state)
        {:error, resp(code, message), state}

      true ->
        candidate = %{state | rcpt_to: state.rcpt_to ++ [to]}

        case policy_check(:rcpt_to, candidate) do
          :ok ->
            {:ok, candidate}

          {:reject, code, message} ->
            {code, message, state} = inc_errors_and_maybe_limit(code, message, state)
            # Don't retain rejected recipients in the envelope.
            {:error, resp(code, message), state}
        end
    end
  end

  @impl true
  def handle_RCPT_extension(_extension, state) do
    {:ok, state}
  end

  @impl true
  def handle_DATA(from, to, data, state) do
    state = inc_commands(state)

    if over_commands?(state) do
      state = schedule_stop(state)
      {:error, resp(421, "Too many commands"), state}
    else
      cond do
        is_nil(state.mail_from) ->
          {code, message, state} =
            inc_errors_and_maybe_limit(503, "Bad sequence of commands", state)

          {:error, resp(code, message), state}

        state.rcpt_to == [] ->
          {code, message, state} =
            inc_errors_and_maybe_limit(503, "Bad sequence of commands", state)

          {:error, resp(code, message), state}

        true ->
          do_handle_DATA(from, to, data, state)
      end
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

            {:error, :queue_full} ->
              emit(:rejected, %{reason: :queue_full})
              {:error, resp(421, "Try again later"), reset_envelope(state)}

            {:error, reason} ->
              Logger.error("queue_error=#{inspect(reason)}")
              emit(:rejected, %{reason: reason})
              {:error, resp(451, "Temporary failure"), reset_envelope(state)}
          end
        end

      {:reject, code, message} ->
        {code, message, state} = inc_errors_and_maybe_limit(code, message, state)
        emit(:rejected, %{reason: message})
        {:error, resp(code, message), reset_envelope(state)}
    end
  end

  defp reset_envelope(state) do
    %{state | mail_from: nil, rcpt_to: []}
  end

  @impl true
  def handle_RSET(state) do
    state = inc_commands(state)

    if over_commands?(state) do
      _ = schedule_stop(state)
    end

    %{state | mail_from: nil, rcpt_to: []}
  end

  @impl true
  def handle_VRFY(_address, state) do
    state = inc_commands(state)

    cond do
      over_commands?(state) ->
        state = schedule_stop(state)
        {:error, resp(421, "Too many commands"), state}

      true ->
        {code, message, state} = inc_errors_and_maybe_limit(252, "VRFY disabled", state)
        {:error, resp(code, message), state}
    end
  end

  @impl true
  def handle_other(_verb, _args, state) do
    state = inc_commands(state)

    cond do
      over_commands?(state) ->
        state = schedule_stop(state)
        {resp(421, "Too many commands"), state}

      true ->
        {code, message, state} =
          inc_errors_and_maybe_limit(500, "Error: command not recognized", state)

        {resp(code, message), state}
    end
  end

  @impl true
  def handle_STARTTLS(state) do
    # gen_smtp handles the on-wire STARTTLS response; this callback only updates state.
    state = inc_commands(state)

    if over_commands?(state) do
      _ = schedule_stop(state)
      state
    else
      if state.tls_active do
        state = inc_errors(state)
        state = maybe_limit_errors(state)
        state
      else
        %{state | tls_active: true}
      end
    end
  end

  @impl true
  def handle_info(:incoming_stop_session, state) do
    {:stop, :normal, state}
  end

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
  def terminate(reason, state) do
    Incoming.ConnectionLimiter.dec(state.peer)
    {:ok, reason, state}
  end

  defp session_from_opts(options) do
    queue = Keyword.get(options, :queue, Incoming.Queue.Disk)
    queue_opts = Keyword.get(options, :queue_opts, [])
    max_message_size = Keyword.get(options, :max_message_size, 10 * 1024 * 1024)
    max_recipients = Keyword.get(options, :max_recipients, 100)
    tls_mode = Keyword.get(options, :tls_mode, :disabled)
    max_commands = Keyword.get(options, :max_commands, 1_000)
    max_errors = Keyword.get(options, :max_errors, 10)
    max_connections_per_ip = Keyword.get(options, :max_connections_per_ip, 10)

    {queue, queue_opts, max_message_size, max_recipients, tls_mode, max_commands, max_errors,
     max_connections_per_ip}
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
    [{String.to_charlist("SIZE"), size} | ext]
  end

  defp maybe_add_starttls(extensions, %{tls_mode: mode, tls_active: false})
       when mode in [:optional, :required] do
    [{String.to_charlist("STARTTLS"), true} | extensions]
  end

  defp maybe_add_starttls(extensions, _state), do: extensions

  defp emit(event, meta) do
    Incoming.Metrics.emit([:incoming, :session, event], %{count: 1}, meta)
  end

  defp resp(code, message), do: "#{code} #{message}"

  defp inc_commands(state), do: %{state | command_count: state.command_count + 1}
  defp inc_errors(state), do: %{state | error_count: state.error_count + 1}

  defp over_commands?(%{max_commands: limit, command_count: count})
       when is_integer(limit) and limit >= 0,
       do: count > limit

  defp over_commands?(_state), do: false

  defp inc_errors_and_maybe_limit(code, message, state) do
    state = inc_errors(state)
    state = maybe_limit_errors(state)

    if is_integer(state.max_errors) and state.max_errors >= 0 and
         state.error_count > state.max_errors do
      {421, "Too many errors", state}
    else
      {code, message, state}
    end
  end

  defp maybe_limit_errors(%{max_errors: limit, error_count: count} = state)
       when is_integer(limit) and limit >= 0 and count > limit do
    schedule_stop(state)
  end

  defp maybe_limit_errors(state), do: state

  defp schedule_stop(state) do
    _ = Process.send_after(self(), :incoming_stop_session, 0)
    state
  end
end
