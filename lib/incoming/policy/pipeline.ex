defmodule Incoming.Policy.Pipeline do
  @moduledoc false

  def run(policies, context) do
    Enum.reduce_while(policies, :ok, fn policy, _acc ->
      case policy.check(context) do
        :ok -> {:cont, :ok}
        {:reject, code, message} -> {:halt, {:reject, code, message}}
      end
    end)
  end
end
