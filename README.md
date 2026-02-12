***CAUTION: AI VIBE CODED PRODUCT***
***EXPERIMENTAL - WORK IN PROGRESS***

Please report weaknesses, flaws, edge cases, or unexpected behavior via GitHub Issues or Discussions.

# Incoming

Production-grade inbound SMTP server library for Elixir. Inbound-only, OTP-native, and designed to replace Postfix for controlled environments.

## Status

Early implementation, actively evolving. Core SMTP, queue, delivery, policies, TLS, and telemetry are in place; streaming DATA is not yet implemented.

## Installation

Add `incoming` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:incoming, "~> 0.5.0"}
  ]
end
```

## Quickstart

```elixir
config :incoming,
  listeners: [
    %{
      name: :default,
      port: 2525,
      tls: :disabled
    }
  ],
  queue: Incoming.Queue.Disk,
  queue_opts: [
    path: "/tmp/incoming",
    fsync: true,
    max_depth: 100_000,
    cleanup_interval_ms: 60_000,
    dead_ttl_seconds: 7 * 24 * 60 * 60
  ],
  delivery: MyApp.IncomingAdapter
```

For a lightweight or ephemeral setup (no disk persistence):

```elixir
config :incoming,
  queue: Incoming.Queue.Memory,
  queue_opts: []
```

Then start your application and point a test SMTP client at `localhost:2525`.

## Policies (Early)

You can configure simple policies in `config.exs`:

```elixir
config :incoming,
  policies: [
    Incoming.Policy.HelloRequired,
    Incoming.Policy.SizeLimit,
    Incoming.Policy.MaxRecipients,
    Incoming.Policy.TlsRequired,
    Incoming.Policy.RateLimiter
  ]
```

### Rate Limiter

The rate limiter uses a sliding time window. Counters reset automatically when the window expires, and a background sweeper periodically cleans up stale entries.

```elixir
config :incoming,
  rate_limit: 5,                       # max requests per window (default: 5)
  rate_limit_window: 60,               # window size in seconds (default: 60)
  rate_limit_max_entries: 100_000,     # cap ETS growth (default: 100_000)
  rate_limit_sweep_interval: 60_000    # sweep interval in ms (default: 60_000)
```

## Queue Backends

Two queue backends are available:

- **`Incoming.Queue.Disk`** (default) — durable, fsync-backed, crash-recoverable. Suitable for production.
- **`Incoming.Queue.Memory`** — ETS-backed, zero-dependency. Messages are lost on restart. Suitable for development, testing, or ephemeral workloads.

### Disk Queue Layout

Messages are stored on disk in:

- `incoming/<id>/` (staging during enqueue; atomically moved into `committed/` when complete)
- `committed/<id>/raw.eml`
- `committed/<id>/meta.json`
- `processing/<id>/` (in-flight delivery)
- `dead/<id>/dead.json` (rejected)

Retries move messages back to `committed/`. Rejects move to `dead/`.

`meta.json` includes an `attempts` counter used to enforce `max_attempts` across restarts.

### Recovery Notes

On application start, the queue runs a recovery pass that:

- Moves any `processing/<id>/` entries back to `committed/` for re-delivery.
- Finalizes crash leftovers by promoting `raw.tmp`/`meta.tmp` to `raw.eml`/`meta.json` where possible.
- Dead-letters incomplete or invalid entries (for example, missing `raw.eml` or `meta.json`, or file entries where a directory is expected) by moving them into `dead/<id>/` with a `dead.json` reason.

## Telemetry (Early)

Events emitted:

- `[:incoming, :message, :queued]` measurements: `%{count: 1}`, metadata: `%{id, size, queue_depth}`
- `[:incoming, :message, :enqueue_error]` measurements: `%{count: 1}`, metadata: `%{id, reason}` (may include `attempted_size`)
- `[:incoming, :delivery, :result]` measurements: `%{count: 1}`, metadata: `%{id, outcome, reason}`
- `[:incoming, :session, :connect]` measurements: `%{count: 1}`, metadata: `%{peer}`
- `[:incoming, :session, :accepted]` measurements: `%{count: 1}`, metadata: `%{id}`
- `[:incoming, :session, :rejected]` measurements: `%{count: 1}`, metadata: `%{reason}`
- `[:incoming, :queue, :depth]` measurements: `%{count}`, metadata: `%{}`

## SMTP Behavior Notes

- Command ordering is enforced: `RCPT` requires a prior `MAIL`, and `DATA` requires at least one `RCPT` (otherwise `503 Bad sequence of commands`).
- After `DATA` completes, the envelope is reset so the next transaction must start with a new `MAIL`.
- Rejected `MAIL`/`RCPT` commands are not retained in the envelope (so rejected recipients do not count toward `max_recipients`).
- RFC 5322 header folding is supported: continuation lines (starting with a space or tab) are unfolded, and duplicate headers are joined with `, `.

## Safety Limits

Incoming includes several safeguards intended for production traffic:

- `max_depth` (queue backpressure): when exceeded, `DATA` responds `421 Try again later`.
- `max_connections_per_ip`: when exceeded, new connections are rejected with `421 Too many connections`.
- `max_commands` / `max_errors`: sessions that exceed limits are disconnected with `421`.

These are configured via `queue_opts`, listener config, and `session_opts` respectively.

## Graceful Shutdown / Drain

Use `Incoming.Control` to stop accepting new connections, drain active sessions, and optionally force close stragglers:

```elixir
Incoming.Control.shutdown(timeout_ms: 5_000)
```

## Delivery Adapter (Early)

Implement `Incoming.DeliveryAdapter` and configure it:

```elixir
config :incoming, delivery: MyApp.IncomingAdapter
```

Return values:

- `:ok` -> message acked
- `{:retry, reason}` -> message requeued with backoff
- `{:reject, reason}` -> message moved to dead-letter

Delivery options (defaults shown):

```elixir
config :incoming,
  delivery_opts: [
    workers: 1,
    poll_interval: 1_000,
    max_attempts: 5,
    base_backoff: 1_000,
    max_backoff: 5_000
  ]
```

## Limitations

- SMTP DATA is fully buffered in memory by `gen_smtp` before we write to disk.
- Large messages can consume significant memory; streaming support is planned.

## Memory, Limits, and Sizing Notes

Incoming currently relies on `gen_smtp` for SMTP parsing and DATA receive. This has two important implications:

1. DATA is received incrementally, but it is still accumulated and flattened into a single binary before the session `DATA` callback is invoked.
2. Message size is still bounded: `gen_smtp` enforces `max_message_size` both:
   - at `MAIL FROM` time when the client provides `SIZE=<n>` (rejects early with `552` if the estimate exceeds the limit)
   - during DATA receive (aborts and returns `552 Message too large` once the limit is exceeded)

### Capacity Math (Rule Of Thumb)

Because DATA is accumulated and then flattened, peak memory per in-flight DATA transaction can be roughly:

`~ 2 * max_message_size` (plus overhead, especially for many small chunks).

This means total worst-case transient memory is approximately:

`concurrent_DATA_sessions * 2 * max_message_size`.

In practice you should treat `max_message_size` and concurrency limits as a pair.

### Practical Recommendations

- Keep `max_message_size` conservative unless you are confident in your VM memory headroom.
- Bound concurrency using listener options:
  - `max_connections` (Ranch connection cap)
  - `num_acceptors` (acceptor concurrency)
- If you need "never buffer the full message in memory", that requires a different SMTP DATA receive strategy than `gen_smtp` currently provides (true streaming support is planned).

## Testing

```bash
mix test
```

## TLS (Early)

Four TLS modes are supported:

- `:disabled` — plaintext only (default)
- `:optional` — advertises STARTTLS; clients may upgrade
- `:required` — advertises STARTTLS; policies can enforce upgrade before `MAIL`
- `:implicit` — connection starts in TLS (port 465 style); STARTTLS is not advertised

### STARTTLS (ports 25/587)

```elixir
config :incoming,
  listeners: [
    %{
      name: :default,
      port: 2525,
      tls: :required,
      tls_opts: [
        certfile: "test/fixtures/test-cert.pem",
        keyfile: "test/fixtures/test-key.pem"
      ]
    }
  ]
```

### Implicit TLS (port 465)

```elixir
config :incoming,
  listeners: [
    %{
      name: :smtps,
      port: 465,
      tls: :implicit,
      tls_opts: [
        certfile: "priv/cert.pem",
        keyfile: "priv/key.pem"
      ]
    }
  ]
```

## Feedback

If you find problems or have suggestions, please open an issue and include:

- Repro steps
- Expected vs actual behavior
- Logs or traces if applicable
