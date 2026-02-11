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
    now = System.monotonic_time(:second)
    window = window_size()

    case :ets.lookup(@table, key) do
      [{^key, count, window_start}] when now - window_start < window ->
        new_count = count + 1
        :ets.insert(@table, {key, new_count, window_start})

        if new_count >= limit() do
          {:reject, 554, "Too many connections"}
        else
          :ok
        end

      _ ->
        :ets.insert(@table, {key, 1, now})
        if 1 >= limit(), do: {:reject, 554, "Too many connections"}, else: :ok
    end
  end

  def check(_context), do: :ok

  defp limit do
    Application.get_env(:incoming, :rate_limit, 5)
  end

  defp window_size do
    Application.get_env(:incoming, :rate_limit_window, 60)
  end
end
