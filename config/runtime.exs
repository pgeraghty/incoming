import Config

parse_int = fn env, default ->
  case System.get_env(env) do
    nil ->
      default

    value ->
      case Integer.parse(value) do
        {int, ""} when int >= 0 -> int
        _ -> raise ArgumentError, "#{env} must be a non-negative integer"
      end
  end
end

parse_bool = fn env, default ->
  case System.get_env(env) do
    nil ->
      default

    value ->
      case String.downcase(value) do
        "1" -> true
        "true" -> true
        "yes" -> true
        "on" -> true
        "0" -> false
        "false" -> false
        "no" -> false
        "off" -> false
        _ -> raise ArgumentError, "#{env} must be a boolean value"
      end
  end
end

incoming_host = System.get_env("INCOMING_HOST", "localhost")
incoming_domain = System.get_env("INCOMING_DOMAIN", incoming_host)

smtp_port = parse_int.("SMTP_PORT", 2525)

smtp_tls_mode =
  case System.get_env("SMTP_TLS_MODE", "disabled") |> String.downcase() do
    "disabled" -> :disabled
    "optional" -> :optional
    "required" -> :required
    "implicit" -> :implicit
    _ -> raise ArgumentError, "SMTP_TLS_MODE must be one of disabled|optional|required|implicit"
  end

smtp_tls_certfile =
  System.get_env("SMTP_TLS_CERTFILE", "/var/lib/lego/certificates/#{incoming_host}.crt")

smtp_tls_keyfile =
  System.get_env("SMTP_TLS_KEYFILE", "/var/lib/lego/certificates/#{incoming_host}.key")

tls_opts =
  if smtp_tls_mode == :disabled do
    []
  else
    [certfile: smtp_tls_certfile, keyfile: smtp_tls_keyfile]
  end

queue_path = System.get_env("QUEUE_PATH", "/var/lib/incoming")
queue_fsync = parse_bool.("QUEUE_FSYNC", true)
queue_max_depth = parse_int.("QUEUE_MAX_DEPTH", 100_000)
queue_cleanup_interval_ms = parse_int.("QUEUE_CLEANUP_INTERVAL_MS", 60_000)
queue_dead_ttl_seconds = parse_int.("QUEUE_DEAD_TTL_SECONDS", 7 * 24 * 60 * 60)

session_max_message_size = parse_int.("SESSION_MAX_MESSAGE_SIZE", 10 * 1024 * 1024)
session_max_recipients = parse_int.("SESSION_MAX_RECIPIENTS", 100)
session_max_commands = parse_int.("SESSION_MAX_COMMANDS", 1_000)
session_max_errors = parse_int.("SESSION_MAX_ERRORS", 10)

listener_max_connections = parse_int.("LISTENER_MAX_CONNECTIONS", 1_000)
listener_max_connections_per_ip = parse_int.("LISTENER_MAX_CONNECTIONS_PER_IP", 10)
listener_num_acceptors = parse_int.("LISTENER_NUM_ACCEPTORS", 10)

delivery_workers = parse_int.("DELIVERY_WORKERS", 1)
delivery_poll_interval = parse_int.("DELIVERY_POLL_INTERVAL_MS", 1_000)
delivery_max_attempts = parse_int.("DELIVERY_MAX_ATTEMPTS", 5)
delivery_base_backoff = parse_int.("DELIVERY_BASE_BACKOFF_MS", 1_000)
delivery_max_backoff = parse_int.("DELIVERY_MAX_BACKOFF_MS", 5_000)

config :incoming,
  domain: incoming_domain,
  listeners: [
    %{
      name: :default,
      domain: incoming_host,
      port: smtp_port,
      tls: smtp_tls_mode,
      tls_opts: tls_opts,
      max_connections: listener_max_connections,
      max_connections_per_ip: listener_max_connections_per_ip,
      num_acceptors: listener_num_acceptors
    }
  ],
  queue: Incoming.Queue.Disk,
  queue_opts: [
    path: queue_path,
    fsync: queue_fsync,
    max_depth: queue_max_depth,
    cleanup_interval_ms: queue_cleanup_interval_ms,
    dead_ttl_seconds: queue_dead_ttl_seconds
  ],
  session_opts: [
    max_message_size: session_max_message_size,
    max_recipients: session_max_recipients,
    max_commands: session_max_commands,
    max_errors: session_max_errors
  ],
  # Demo image defaults to queue-only behavior. Users can set this in a custom build.
  delivery: nil,
  delivery_opts: [
    workers: delivery_workers,
    poll_interval: delivery_poll_interval,
    max_attempts: delivery_max_attempts,
    base_backoff: delivery_base_backoff,
    max_backoff: delivery_max_backoff
  ]
