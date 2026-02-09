defmodule IncomingTest.DummyAdapter do
  @behaviour Incoming.DeliveryAdapter

  def set_mode(mode) do
    :persistent_term.put({__MODULE__, :mode}, mode)
    :ok
  end

  @impl true
  def deliver(_message) do
    case :persistent_term.get({__MODULE__, :mode}, :ok) do
      :ok -> :ok
      :retry -> {:retry, :temporary}
      :reject -> {:reject, :permanent}
    end
  end
end
