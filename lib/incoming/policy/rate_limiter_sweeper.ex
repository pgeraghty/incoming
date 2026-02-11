defmodule Incoming.Policy.RateLimiterSweeper do
  @moduledoc false

  use GenServer

  @table :incoming_rate_limits

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    schedule_sweep()
    {:ok, %{}}
  end

  @impl true
  def handle_info(:sweep, state) do
    sweep()
    schedule_sweep()
    {:noreply, state}
  end

  def sweep do
    Incoming.Policy.RateLimiter.init_table()
    now = System.monotonic_time(:second)
    window = Application.get_env(:incoming, :rate_limit_window, 60)

    :ets.select_delete(@table, [
      {{:_, :_, :"$1"}, [{:<, :"$1", now - window}], [true]}
    ])
  end

  defp schedule_sweep do
    interval = Application.get_env(:incoming, :rate_limit_sweep_interval, 60_000)
    Process.send_after(self(), :sweep, interval)
  end
end
