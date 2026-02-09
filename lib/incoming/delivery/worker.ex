defmodule Incoming.Delivery.Worker do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :poll_interval, 1_000)
    max_attempts = Keyword.get(opts, :max_attempts, 5)
    base_backoff = Keyword.get(opts, :base_backoff, 1_000)
    max_backoff = Keyword.get(opts, :max_backoff, 5_000)

    state = %{
      interval: interval,
      max_attempts: max_attempts,
      base_backoff: base_backoff,
      max_backoff: max_backoff
    }

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
          result =
            try do
              Incoming.Delivery.Dispatcher.deliver_sync(message)
            rescue
              e ->
                {:retry, {:exception, e.__struct__, Exception.message(e)}}
            catch
              kind, reason ->
                {:retry, {kind, reason}}
            end

          {new_state, delay} = handle_result(queue, message, result, state)
          schedule_tick(delay)
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
    {state, state.interval}
  end

  defp handle_result(
         queue,
         %Incoming.Message{id: id, attempts: attempts},
         {:retry, reason},
         state
       ) do
    attempt = (attempts || 0) + 1

    if attempt >= state.max_attempts do
      emit(:reject, id, {:max_attempts, reason})
      queue.nack(id, :reject, {:max_attempts, reason})
      {state, state.interval}
    else
      emit(:retry, id, reason)
      queue.nack(id, :retry, reason)
      backoff = backoff_for(attempt, state.base_backoff, state.max_backoff)
      {state, state.interval + backoff}
    end
  end

  defp handle_result(queue, %Incoming.Message{id: id}, {:reject, reason}, state) do
    emit(:reject, id, reason)
    queue.nack(id, :reject, reason)
    {state, state.interval}
  end

  defp handle_result(_queue, _message, _result, state), do: {state, state.interval}

  defp emit(outcome, id, reason) do
    Incoming.Metrics.emit([:incoming, :delivery, :result], %{count: 1}, %{
      id: id,
      outcome: outcome,
      reason: reason
    })
  end

  defp backoff_for(attempt, base, max_backoff) do
    backoff = trunc(base * :math.pow(2, attempt - 1))
    min(backoff, max_backoff)
  end

  defp schedule_tick(delay) do
    Process.send_after(self(), :tick, delay)
  end
end
