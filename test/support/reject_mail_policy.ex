defmodule IncomingTest.RejectMailPolicy do
  @behaviour Incoming.Policy

  @impl true
  def check(%{phase: :mail_from}), do: {:reject, 550, "MAIL rejected"}
  def check(_ctx), do: :ok
end
