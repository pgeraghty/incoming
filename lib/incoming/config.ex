defmodule Incoming.Config do
  @moduledoc false

  @default_listener %{
    name: :default,
    port: 2525,
    tls: :disabled
  }

  @default_queue_opts [
    path: "/tmp/incoming",
    fsync: true
  ]

  def listeners do
    Application.get_env(:incoming, :listeners, [@default_listener])
  end

  def queue_module do
    Application.get_env(:incoming, :queue, Incoming.Queue.Disk)
  end

  def queue_opts do
    Application.get_env(:incoming, :queue_opts, @default_queue_opts)
  end
end
