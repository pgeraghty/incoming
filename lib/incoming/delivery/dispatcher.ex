defmodule Incoming.Delivery.Dispatcher do
  @moduledoc false

  def dispatch(message) do
    adapter = Application.get_env(:incoming, :delivery)

    if adapter do
      _ = Task.start(fn -> invoke(adapter, message) end)
    end

    :ok
  end

  def deliver_sync(message) do
    adapter = Application.get_env(:incoming, :delivery)

    if adapter do
      invoke(adapter, message)
    else
      :ok
    end
  end

  defp invoke(adapter, message) do
    if function_exported?(adapter, :pre_deliver, 1) do
      _ = adapter.pre_deliver(message)
    end

    result = adapter.deliver(message)

    if function_exported?(adapter, :post_deliver, 2) do
      _ = adapter.post_deliver(message, result)
    end

    result
  end
end
