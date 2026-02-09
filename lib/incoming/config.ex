defmodule Incoming.Config do
  @moduledoc false

  @default_listener %{
    name: :default,
    port: 2525,
    tls: :disabled,
    tls_opts: [],
    max_connections: 1_000
  }

  @default_queue_opts [
    path: "/tmp/incoming",
    fsync: true
  ]

  @default_session_opts [
    max_message_size: 10 * 1024 * 1024,
    max_recipients: 100
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

  def session_opts do
    Application.get_env(:incoming, :session_opts, @default_session_opts)
  end

  def policies do
    Application.get_env(:incoming, :policies, [])
  end

  def delivery_adapter do
    Application.get_env(:incoming, :delivery)
  end

  def delivery_opts do
    Application.get_env(:incoming, :delivery_opts,
      workers: 1,
      poll_interval: 1_000,
      max_attempts: 5,
      base_backoff: 1_000,
      max_backoff: 5_000
    )
  end
end
