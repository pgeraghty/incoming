defmodule IncomingTest.RejectRcptOncePolicy do
  @behaviour Incoming.Policy

  def reset do
    :persistent_term.put({__MODULE__, :rejected?}, false)
  end

  @impl true
  def check(%{phase: :rcpt_to}) do
    rejected? = :persistent_term.get({__MODULE__, :rejected?}, false)

    if rejected? do
      :ok
    else
      :persistent_term.put({__MODULE__, :rejected?}, true)
      {:reject, 550, "RCPT rejected"}
    end
  end

  def check(_ctx), do: :ok
end
