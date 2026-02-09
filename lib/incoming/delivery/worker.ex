defmodule Incoming.Delivery.Worker do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :poll_interval, 1_000)
    state = %{interval: interval, attempts: %{}}
    schedule_tick(0)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    adapter = Incoming.Config.delivery_adapter()
    queue = Incoming.Config.queue_module()

    if adapter do
      case queue.dequeue() do
        {:ok, message} ->
          result = Incoming.Delivery.Dispatcher.deliver_sync(message)
          new_state = handle_result(queue, message, result, state)
          schedule_tick(new_state.interval)
          {:noreply, new_state}

        {:empty} ->
          schedule_tick(state.interval)
          {:noreply, state}
      end
    else
      schedule_tick(state.interval)
      {:noreply, state}
    end
  end

  defp handle_result(queue, %Incoming.Message{id: id}, :ok, state) do
    emit(:ok, id, nil)
    queue.ack(id)
    %{state | attempts: Map.delete(state.attempts, id)}
  end

  defp handle_result(queue, %Incoming.Message{id: id}, {:retry, reason}, state) do
    attempt = Map.get(state.attempts, id, 0) + 1
    emit(:retry, id, reason)
    queue.nack(id, :retry, reason)
    backoff = min(1_000 * attempt, 5_000)
    Process.sleep(backoff)
    %{state | attempts: Map.put(state.attempts, id, attempt)}
  end

  defp handle_result(queue, %Incoming.Message{id: id}, {:reject, reason}, state) do
    emit(:reject, id, reason)
    queue.nack(id, :reject, reason)
    %{state | attempts: Map.delete(state.attempts, id)}
  end

  defp handle_result(_queue, _message, _result, state), do: state

  defp emit(outcome, id, reason) do
    :telemetry.execute([:incoming, :delivery, :result], %{count: 1}, %{
      id: id,
      outcome: outcome,
      reason: reason
    })
  end

  defp schedule_tick(delay) do
    Process.send_after(self(), :tick, delay)
  end
end
