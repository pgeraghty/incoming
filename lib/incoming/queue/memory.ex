defmodule Incoming.Queue.Memory do
  @moduledoc false

  @behaviour Incoming.Queue

  use GenServer

  @table :incoming_memory_queue
  # Secondary index: {message_id} -> {seq} for lookups by id
  @index :incoming_memory_queue_index

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :ordered_set])
    :ets.new(@index, [:named_table, :public, :set])
    schedule_depth_telemetry()
    {:ok, %{}}
  end

  @impl GenServer
  def handle_info(:emit_depth, state) do
    Incoming.Metrics.emit([:incoming, :queue, :depth], %{count: depth()}, %{})
    schedule_depth_telemetry()
    {:noreply, state}
  end

  @impl Incoming.Queue
  def enqueue(from, to, data, opts) do
    max_message_size = Keyword.get(opts, :max_message_size, nil)

    if is_integer(max_message_size) and byte_size(data) > max_message_size do
      id = Incoming.Id.generate()

      Incoming.Metrics.emit([:incoming, :message, :enqueue_error], %{count: 1}, %{
        id: id,
        reason: :message_too_large,
        attempted_size: byte_size(data)
      })

      {:error, :message_too_large}
    else
      do_enqueue(from, to, data)
    end
  end

  defp do_enqueue(from, to, data) do
    id = Incoming.Id.generate()
    seq = System.unique_integer([:monotonic, :positive])
    received_at = DateTime.utc_now()

    message = %Incoming.Message{
      id: id,
      mail_from: from,
      rcpt_to: to,
      received_at: received_at,
      raw_data: data,
      attempts: 0
    }

    :ets.insert(@table, {seq, id, :committed, message, data})
    :ets.insert(@index, {id, seq})

    Incoming.Metrics.emit([:incoming, :message, :queued], %{count: 1}, %{
      id: id,
      size: byte_size(data),
      queue_depth: depth()
    })

    {:ok, message}
  end

  @impl Incoming.Queue
  def dequeue do
    match_spec = [{{:"$1", :"$2", :committed, :"$3", :"$4"}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}]

    case :ets.select(@table, match_spec, 1) do
      {[{seq, _id, message, _data}], _cont} ->
        :ets.update_element(@table, seq, {3, :processing})
        {:ok, message}

      :"$end_of_table" ->
        {:empty}
    end
  end

  @impl Incoming.Queue
  def ack(message_id) do
    case :ets.lookup(@index, message_id) do
      [{^message_id, seq}] ->
        :ets.delete(@table, seq)
        :ets.delete(@index, message_id)

      [] ->
        :ok
    end

    :ok
  end

  @impl Incoming.Queue
  def nack(message_id, action, _reason \\ nil) do
    case action do
      :retry ->
        case :ets.lookup(@index, message_id) do
          [{^message_id, seq}] ->
            case :ets.lookup(@table, seq) do
              [{^seq, ^message_id, _status, message, data}] ->
                updated = %{message | attempts: message.attempts + 1}
                :ets.insert(@table, {seq, message_id, :committed, updated, data})

              [] ->
                :ok
            end

          [] ->
            :ok
        end

        :ok

      :reject ->
        case :ets.lookup(@index, message_id) do
          [{^message_id, seq}] ->
            :ets.delete(@table, seq)
            :ets.delete(@index, message_id)

          [] ->
            :ok
        end

        :ok
    end
  end

  @impl Incoming.Queue
  def depth do
    :ets.select_count(@table, [{{:_, :_, :committed, :_, :_}, [], [true]}])
  end

  @impl Incoming.Queue
  def recover do
    match_spec = [{{:"$1", :"$2", :processing, :"$3", :"$4"}, [], [{{:"$1", :"$2", :"$3", :"$4"}}]}]

    :ets.select(@table, match_spec)
    |> Enum.each(fn {seq, id, message, data} ->
      :ets.insert(@table, {seq, id, :committed, message, data})
    end)

    :ok
  end

  defp schedule_depth_telemetry do
    Process.send_after(self(), :emit_depth, 5_000)
  end
end
