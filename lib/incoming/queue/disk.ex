defmodule Incoming.Queue.Disk do
  @moduledoc false

  @behaviour Incoming.Queue

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    opts = Incoming.Validate.queue_opts!(opts)
    path = Keyword.get(opts, :path, "/tmp/incoming")
    fsync = Keyword.get(opts, :fsync, true)
    ensure_dirs(path)
    ensure_writable!(path)
    schedule_depth_telemetry()
    {:ok, %{path: path, fsync: fsync}}
  end

  @impl true
  def handle_info(:emit_depth, state) do
    Incoming.Metrics.emit([:incoming, :queue, :depth], %{count: depth()}, %{})
    schedule_depth_telemetry()
    {:noreply, state}
  end

  @impl true
  def enqueue(from, to, data, opts) do
    enqueue_stream(from, to, [data], opts)
  end

  @impl true
  def enqueue_stream(from, to, chunks, opts) do
    path = Keyword.get(opts, :path, "/tmp/incoming")
    fsync = Keyword.get(opts, :fsync, true)
    max_message_size = Keyword.get(opts, :max_message_size, nil)

    id = Incoming.Id.generate()
    base = Path.join([path, "committed", id])
    raw_path = Path.join(base, "raw.eml")
    raw_tmp_path = Path.join(base, "raw.tmp")
    meta_path = Path.join(base, "meta.json")
    meta_tmp_path = Path.join(base, "meta.tmp")
    received_at = DateTime.utc_now()

    try do
      _ = Incoming.Validate.queue_opts!(opts)
      ensure_dirs(path)
      File.mkdir_p!(base)

      size =
        File.open!(raw_tmp_path, [:write, :binary], fn io ->
          Enum.reduce_while(chunks, 0, fn chunk, acc ->
            chunk_size = IO.iodata_length(chunk)
            new_size = acc + chunk_size

            if is_integer(max_message_size) and new_size > max_message_size do
              {:halt, {:too_large, new_size}}
            else
              :ok = IO.binwrite(io, chunk)
              {:cont, new_size}
            end
          end)
        end)

      case size do
        {:too_large, _size} ->
          File.rm_rf!(base)
          {:error, :message_too_large}

        size when is_integer(size) ->
          case File.rename(raw_tmp_path, raw_path) do
            :ok ->
              :ok

            {:error, reason} ->
              File.rm_rf!(base)
              {:error, reason}
          end
          |> case do
            {:error, reason} ->
              {:error, reason}

            :ok ->
              meta_payload =
                Jason.encode!(%{
                  id: id,
                  mail_from: from,
                  rcpt_to: to,
                  received_at: received_at |> DateTime.to_iso8601(),
                  size: size
                })

              File.write!(meta_tmp_path, meta_payload)

              case File.rename(meta_tmp_path, meta_path) do
                :ok ->
                  if fsync do
                    :ok = fsync_file(raw_path)
                    :ok = fsync_file(meta_path)
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

                  Incoming.Metrics.emit([:incoming, :message, :queued], %{count: 1}, %{
                    id: id,
                    size: size,
                    queue_depth: depth()
                  })

                  {:ok, message}

                {:error, reason} ->
                  File.rm_rf!(base)
                  {:error, reason}
              end
          end
      end
    rescue
      e ->
        _ = File.rm_rf(base)
        {:error, e}
    end
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

  defp fsync_file(path) do
    case File.open(path, [:read]) do
      {:ok, io} ->
        _ = :file.sync(io)
        File.close(io)
        :ok

      _ ->
        :ok
    end
  end

  @impl true
  def dequeue do
    path = state_path()
    ensure_dirs(path)
    committed = Path.join(path, "committed")
    processing = Path.join(path, "processing")
    dead = Path.join(path, "dead")

    case list_ids(committed) do
      [] ->
        {:empty}

      ids ->
        Enum.reduce_while(ids, {:empty}, fn id, _acc ->
          from = Path.join(committed, id)

          case load_message(from, id) do
            {:ok, message} ->
              to = Path.join(processing, id)

              case File.rename(from, to) do
                :ok ->
                  message = %{
                    message
                    | raw_path: Path.join(to, "raw.eml"),
                      meta_path: Path.join(to, "meta.json")
                  }

                  {:halt, {:ok, message}}

                {:error, reason} ->
                  move_to_dead(from, dead, id, {:rename_failed, reason})
                  {:cont, {:empty}}
              end

            {:error, reason} ->
              move_to_dead(from, dead, id, reason)
              {:cont, {:empty}}
          end
        end)
    end
  end

  @impl true
  def ack(message_id) do
    path = state_path()
    ensure_dirs(path)
    File.rm_rf(Path.join([path, "processing", message_id]))
    :ok
  end

  @impl true
  def nack(message_id, action, reason \\ nil) do
    path = state_path()
    ensure_dirs(path)
    from = Path.join([path, "processing", message_id])

    case action do
      :retry ->
        File.rename(from, Path.join([path, "committed", message_id]))
        :ok

      :reject ->
        dead_dir = Path.join([path, "dead", message_id])
        File.rename(from, dead_dir)
        write_dead_reason(dead_dir, reason)
        :ok
    end
  end

  @impl true
  def depth do
    path = state_path()
    ensure_dirs(path)
    committed = Path.join(path, "committed")
    length(list_ids(committed))
  end

  @impl true
  def recover do
    path = state_path()
    ensure_dirs(path)
    processing = Path.join(path, "processing")
    committed = Path.join(path, "committed")

    for id <- list_ids(processing) do
      File.rename(Path.join(processing, id), Path.join([path, "committed", id]))
    end

    # Clean up crash leftovers in committed entries.
    for id <- list_ids(committed) do
      base = Path.join(committed, id)

      if File.dir?(base) do
        raw = Path.join(base, "raw.eml")
        raw_tmp = Path.join(base, "raw.tmp")
        meta = Path.join(base, "meta.json")
        meta_tmp = Path.join(base, "meta.tmp")

        _ = recover_tmp(raw_tmp, raw)
        _ = recover_tmp(meta_tmp, meta)
      end
    end

    :ok
  end

  defp recover_tmp(tmp_path, final_path) do
    cond do
      File.exists?(final_path) ->
        :ok

      File.exists?(tmp_path) ->
        case File.rename(tmp_path, final_path) do
          :ok -> :ok
          _ -> :ok
        end

      true ->
        :ok
    end
  end

  defp list_ids(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        entries
        |> Enum.sort()

      _ ->
        []
    end
  end

  defp ensure_dirs(path) do
    File.mkdir_p!(Path.join(path, "committed"))
    File.mkdir_p!(Path.join(path, "processing"))
    File.mkdir_p!(Path.join(path, "dead"))
  end

  defp ensure_writable!(path) do
    test_path = Path.join(path, ".incoming_write_test")
    File.write!(test_path, "ok")
    File.rm!(test_path)
  end

  defp schedule_depth_telemetry do
    Process.send_after(self(), :emit_depth, 5_000)
  end

  defp write_dead_reason(dir, reason) do
    payload = %{
      rejected_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      reason: inspect(reason)
    }

    File.write(Path.join(dir, "dead.json"), Jason.encode!(payload))
  end

  defp load_message(dir, id) do
    meta_path = Path.join(dir, "meta.json")
    raw_path = Path.join(dir, "raw.eml")

    case File.stat(raw_path) do
      {:ok, _} ->
        case File.read(meta_path) do
          {:ok, meta} ->
            case Jason.decode(meta) do
              {:ok, decoded} ->
                received_at = parse_time(decoded["received_at"])

                {:ok,
                 %Incoming.Message{
                   id: id,
                   mail_from: decoded["mail_from"],
                   rcpt_to: decoded["rcpt_to"] || [],
                   received_at: received_at,
                   raw_path: raw_path,
                   meta_path: meta_path
                 }}

              _ ->
                {:error, :invalid_metadata}
            end

          _ ->
            {:error, :missing_meta}
        end

      _ ->
        {:error, :missing_raw}
    end
  end

  defp parse_time(nil), do: DateTime.utc_now()

  defp parse_time(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _} -> dt
      _ -> DateTime.utc_now()
    end
  end

  defp state_path do
    Application.get_env(:incoming, :queue_opts, path: "/tmp/incoming")
    |> Keyword.get(:path, "/tmp/incoming")
  end

  defp move_to_dead(from, dead_root, id, reason) do
    dead_dir = Path.join(dead_root, id)

    if File.exists?(dead_dir) do
      _ = File.rm_rf(dead_dir)
    end

    if File.dir?(from) do
      case File.rename(from, dead_dir) do
        :ok -> write_dead_reason(dead_dir, reason)
        _ -> :ok
      end
    else
      _ = File.mkdir_p(dead_dir)
      _ = File.rename(from, Path.join(dead_dir, "entry"))
      write_dead_reason(dead_dir, reason)
    end
  end
end
