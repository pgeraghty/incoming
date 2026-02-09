defmodule IncomingTest.RejectDataPolicy do
  @behaviour Incoming.Policy

  @impl true
  def check(%{phase: :data_start}), do: {:reject, 554, "Data rejected"}
  def check(_ctx), do: :ok
end
