defmodule Incoming.Metrics do
  @moduledoc false

  def emit(event, measurements, metadata) do
    if Code.ensure_loaded?(:telemetry) and function_exported?(:telemetry, :execute, 3) do
      :telemetry.execute(event, measurements, metadata)
    end
  end
end
