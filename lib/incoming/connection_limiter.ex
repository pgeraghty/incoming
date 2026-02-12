defmodule Incoming.ConnectionLimiter do
  @moduledoc false

  @table :incoming_connections_per_ip

  def init_table do
    case :ets.info(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
        rescue
          _ -> :ok
        end

      _ ->
        :ok
    end
  end

  def inc(_peer, limit) when limit in [nil, :infinity], do: :ok

  def inc(peer, limit) when is_integer(limit) and limit > 0 do
    init_table()

    # :ets.update_counter is atomic and creates the row if missing.
    count = :ets.update_counter(@table, peer, {2, 1}, {peer, 0})

    if count > limit do
      _ = :ets.update_counter(@table, peer, {2, -1}, {peer, 0})
      {:error, :limit_exceeded}
    else
      :ok
    end
  end

  def inc(_peer, _limit), do: :ok

  def dec(peer) do
    init_table()

    case :ets.lookup(@table, peer) do
      [{^peer, 1}] ->
        :ets.delete(@table, peer)
        :ok

      [{^peer, count}] when is_integer(count) and count > 1 ->
        _ = :ets.update_counter(@table, peer, {2, -1}, {peer, 0})
        :ok

      _ ->
        :ok
    end
  end
end
