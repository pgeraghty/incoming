defmodule Incoming.Queue.Disk do
  @moduledoc false

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

  def enqueue(from, to, data, opts) do
    path = Keyword.get(opts, :path, "/tmp/incoming")
    fsync = Keyword.get(opts, :fsync, true)
    id = Incoming.Id.generate()

    base = Path.join([path, "committed", id])
    File.mkdir_p!(base)

    raw_path = Path.join(base, "raw.eml")
    meta_path = Path.join(base, "meta.json")

    File.write!(raw_path, data)
    File.write!(meta_path, Jason.encode!(%{
      id: id,
      mail_from: from,
      rcpt_to: to,
      received_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }))

    if fsync do
      :ok = fsync_dir(base)
    end

    {:ok, id}
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
end
