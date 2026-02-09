defmodule IncomingTest.CapturePolicy do
  @behaviour Incoming.Policy

  def set_target(pid) do
    :persistent_term.put({__MODULE__, :pid}, pid)
  end

  @impl true
  def check(%{phase: phase} = ctx) do
    case :persistent_term.get({__MODULE__, :pid}, nil) do
      nil -> :ok
      pid -> send(pid, {:policy_phase, phase, ctx})
    end

    :ok
  end
end
