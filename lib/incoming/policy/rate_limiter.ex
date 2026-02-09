defmodule Incoming.Policy.RateLimiter do
  @moduledoc false

  @behaviour Incoming.Policy

  @table :incoming_rate_limits

  def init_table do
    case :ets.info(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:named_table, :public, :set])
        rescue
          _ -> :ok
        end

      _ ->
        :ok
    end
  end

  @impl true
  def check(%{phase: :mail_from, peer: peer}) do
    init_table()
    key = {peer, :mail_from}
    count = :ets.update_counter(@table, key, {2, 1}, {key, 0})

    if count >= limit() do
      {:reject, 554, "Too many connections"}
    else
      :ok
    end
  end

  def check(_context), do: :ok

  defp limit do
    Application.get_env(:incoming, :rate_limit, 5)
  end
end
