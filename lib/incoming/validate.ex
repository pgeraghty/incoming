defmodule Incoming.Validate do
  @moduledoc false

  def listener!(%{port: port} = listener) when is_integer(port) and port > 0 and port < 65536 do
    tls = Map.get(listener, :tls, :disabled)
    tls_opts = Map.get(listener, :tls_opts, [])

    if tls in [:optional, :required] and
         not (Keyword.has_key?(tls_opts, :certfile) and Keyword.has_key?(tls_opts, :keyfile)) do
      raise ArgumentError, "tls_opts must include :certfile and :keyfile when tls is enabled"
    end

    listener
  end

  def listener!(_listener) do
    raise ArgumentError, "listener must include a valid :port"
  end

  def queue_opts!(opts) do
    path = Keyword.get(opts, :path, "/tmp/incoming")

    unless is_binary(path) and String.length(path) > 0 do
      raise ArgumentError, "queue_opts :path must be a non-empty string"
    end

    opts
  end
end
