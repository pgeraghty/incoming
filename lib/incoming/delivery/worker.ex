defmodule Incoming.Delivery.Worker do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :poll_interval, 1_000)
    state = %{interval: interval}
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
          handle_result(queue, message, result)

        {:empty} ->
          :ok
      end
    end

    schedule_tick(state.interval)
    {:noreply, state}
  end

  defp handle_result(queue, %Incoming.Message{id: id}, :ok), do: queue.ack(id)
  defp handle_result(queue, %Incoming.Message{id: id}, {:retry, _}), do: queue.nack(id, :retry)
  defp handle_result(queue, %Incoming.Message{id: id}, {:reject, _}), do: queue.nack(id, :reject)
  defp handle_result(_queue, _message, _), do: :ok

  defp schedule_tick(delay) do
    Process.send_after(self(), :tick, delay)
  end
end
