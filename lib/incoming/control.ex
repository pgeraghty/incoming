defmodule Incoming.Control do
  @moduledoc """
  Operational controls for listeners and sessions.

  This module is intended for controlled shutdowns:

  1. Stop accepting new connections (stop Ranch listeners).
  2. Drain existing sessions with a timeout.
  3. Force-stop any remaining sessions.
  """

  def stop_accepting do
    listeners()
    |> Enum.each(fn name ->
      _ = :gen_smtp_server.stop(name)
    end)

    :ok
  end

  def sessions do
    listeners()
    |> Enum.map(fn name -> {name, :gen_smtp_server.sessions(name)} end)
    |> Map.new()
  end

  def drain(timeout_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_drain(deadline)
  end

  def force_close do
    sessions()
    |> Map.values()
    |> List.flatten()
    |> Enum.each(fn pid ->
      _ =
        try do
          GenServer.call(pid, :stop, 1_000)
        catch
          _, _ -> :ok
        end
    end)

    :ok
  end

  def shutdown(opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 5_000)

    :ok = stop_accepting()

    case drain(timeout_ms) do
      :ok ->
        :ok

      {:error, {:timeout, remaining}} ->
        _ = force_close()
        {:error, {:timeout, remaining}}
    end
  end

  defp do_drain(deadline) do
    remaining =
      sessions()
      |> Enum.flat_map(fn {_name, pids} -> pids end)
      |> Enum.uniq()

    cond do
      remaining == [] ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, {:timeout, remaining}}

      true ->
        Process.sleep(50)
        do_drain(deadline)
    end
  end

  defp listeners do
    Incoming.Config.listeners()
    |> Enum.map(&Map.fetch!(&1, :name))
  end
end
