defmodule Incoming.Session do
  @moduledoc false

  @behaviour :gen_smtp_server_session

  require Logger

  defstruct hostname: nil,
            peer: nil,
            queue: Incoming.Queue.Disk,
            queue_opts: [],
            mail_from: nil,
            rcpt_to: []

  @impl true
  def init(hostname, _session_count, peer, options) do
    {queue, queue_opts} = queue_from_opts(options)
    banner = [hostname, " ESMTP incoming"]

    state = %__MODULE__{
      hostname: hostname,
      peer: peer,
      queue: queue,
      queue_opts: queue_opts
    }

    {:ok, banner, state}
  end

  @impl true
  def handle_HELO(_hostname, state) do
    {:ok, state}
  end

  @impl true
  def handle_EHLO(_hostname, extensions, state) do
    {:ok, extensions, state}
  end

  @impl true
  def handle_MAIL(from, state) do
    {:ok, %{state | mail_from: from, rcpt_to: []}}
  end

  @impl true
  def handle_MAIL_extension(_extension, state) do
    {:ok, state}
  end

  @impl true
  def handle_RCPT(to, state) do
    {:ok, %{state | rcpt_to: state.rcpt_to ++ [to]}}
  end

  @impl true
  def handle_RCPT_extension(_extension, state) do
    {:ok, state}
  end

  @impl true
  def handle_DATA(from, to, data, state) do
    case state.queue.enqueue(from, to, data, state.queue_opts) do
      {:ok, id} ->
        {:ok, "Ok: queued as <#{id}>", state}

      {:error, reason} ->
        Logger.error("queue_error=#{inspect(reason)}")
        {:error, "451 Temporary failure", state}
    end
  end

  @impl true
  def handle_RSET(state) do
    %{state | mail_from: nil, rcpt_to: []}
  end

  @impl true
  def handle_VRFY(_address, state) do
    {:error, "252 VRFY disabled", state}
  end

  @impl true
  def handle_other(_verb, _args, state) do
    {"500 Error: command not recognized", state}
  end

  @impl true
  def handle_STARTTLS(state) do
    state
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
    {queue, queue_opts}
  end
end
