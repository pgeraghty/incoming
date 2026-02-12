defmodule Incoming.Queue.DiskGC do
  @moduledoc false

  use GenServer

  require Logger

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    schedule()
    {:ok, %{}}
  end

  @impl true
  def handle_info(:gc, state) do
    _ = gc_once()
    schedule()
    {:noreply, state}
  end

  def gc_once do
    opts = Incoming.Config.queue_opts()
    ttl = Keyword.get(opts, :dead_ttl_seconds, nil)

    cond do
      ttl in [nil, :infinity] ->
        :ok

      not (is_integer(ttl) and ttl >= 0) ->
        :ok

      true ->
        path = Keyword.get(opts, :path, "/tmp/incoming")
        dead_root = Path.join(path, "dead")
        now = System.system_time(:second)
        cutoff = now - ttl
        max_deletes = Keyword.get(opts, :dead_gc_max_deletes, 1_000)

        do_gc(dead_root, cutoff, max_deletes)
    end
  end

  defp do_gc(dead_root, cutoff, max_deletes) do
    with true <- File.dir?(dead_root),
         {:ok, entries} <- File.ls(dead_root) do
      entries
      |> Enum.take(max_deletes)
      |> Enum.each(fn entry ->
        dir = Path.join(dead_root, entry)

        if File.dir?(dir) do
          ts = dead_timestamp(dir)

          if is_integer(ts) and ts < cutoff do
            _ = File.rm_rf(dir)
          end
        end
      end)

      :ok
    else
      _ ->
        :ok
    end
  rescue
    e ->
      Logger.debug("queue_gc_error=#{inspect({e.__struct__, Exception.message(e)})}")
      :ok
  end

  defp dead_timestamp(dir) do
    dead_json = Path.join(dir, "dead.json")

    with true <- File.exists?(dead_json),
         {:ok, body} <- File.read(dead_json),
         {:ok, decoded} <- Jason.decode(body),
         rejected_at when is_binary(rejected_at) <- decoded["rejected_at"],
         {:ok, dt, _} <- DateTime.from_iso8601(rejected_at) do
      DateTime.to_unix(dt)
    else
      _ -> file_mtime_seconds(dead_json) || file_mtime_seconds(dir)
    end
  end

  defp file_mtime_seconds(path) do
    case File.stat(path) do
      {:ok, %File.Stat{mtime: mtime}} ->
        mtime
        |> NaiveDateTime.from_erl!()
        |> DateTime.from_naive!("Etc/UTC")
        |> DateTime.to_unix()

      _ ->
        nil
    end
  rescue
    _ -> nil
  end

  defp schedule do
    interval = Incoming.Config.queue_opts() |> Keyword.get(:cleanup_interval_ms, 60_000)

    if is_integer(interval) and interval > 0 do
      Process.send_after(self(), :gc, interval)
    end

    :ok
  end
end
