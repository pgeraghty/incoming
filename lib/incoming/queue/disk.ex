defmodule Incoming.Queue.Disk do
  @moduledoc false

  @behaviour Incoming.Queue

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "/tmp/incoming")
    fsync = Keyword.get(opts, :fsync, true)
    File.mkdir_p!(Path.join(path, "committed"))
    {:ok, %{path: path, fsync: fsync}}
  end

  @impl true
  def enqueue(from, to, data, opts) do
    path = Keyword.get(opts, :path, "/tmp/incoming")
    fsync = Keyword.get(opts, :fsync, true)
    id = Incoming.Id.generate()

    base = Path.join([path, "committed", id])
    File.mkdir_p!(base)

    raw_path = Path.join(base, "raw.eml")
    meta_path = Path.join(base, "meta.json")
    received_at = DateTime.utc_now()

    File.write!(raw_path, data)
    File.write!(meta_path, Jason.encode!(%{
      id: id,
      mail_from: from,
      rcpt_to: to,
      received_at: received_at |> DateTime.to_iso8601()
    }))

    if fsync do
      :ok = fsync_dir(base)
    end

    message = %Incoming.Message{
      id: id,
      mail_from: from,
      rcpt_to: to,
      received_at: received_at,
      raw_path: raw_path,
      meta_path: meta_path
    }

    if Code.ensure_loaded?(:telemetry) and function_exported?(:telemetry, :execute, 3) do
      :telemetry.execute([:incoming, :message, :queued], %{count: 1}, %{
        id: id,
        size: byte_size(data)
      })
    end

    {:ok, message}
  end

  defp fsync_dir(dir) do
    case File.open(dir, [:read]) do
      {:ok, io} ->
        _ = :file.sync(io)
        File.close(io)
        :ok

      _ ->
        :ok
    end
  end

  @impl true
  def dequeue, do: {:empty}

  @impl true
  def ack(_message_id), do: :ok

  @impl true
  def nack(_message_id, _action), do: :ok

  @impl true
  def depth, do: 0

  @impl true
  def recover, do: :ok
end
