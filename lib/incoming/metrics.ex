defmodule Incoming.Metrics do
  @moduledoc false

  def emit(event, measurements, metadata) do
    :telemetry.execute(rewrite_event(event), measurements, metadata)
  end

  defp rewrite_event([:incoming | rest]) do
    prefix = Application.get_env(:incoming, :telemetry_prefix, [:incoming])
    List.wrap(prefix) ++ rest
  end

  defp rewrite_event(event), do: event
end
