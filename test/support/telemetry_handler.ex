defmodule IncomingTest.TelemetryHandler do
  def handle(event, measurements, metadata, pid) do
    send(pid, {:telemetry, event, measurements, metadata})
  end
end
