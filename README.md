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
    {:incoming, "~> 0.1.0"}
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
  queue_opts: [path: "/tmp/incoming", fsync: true],
  delivery: MyApp.IncomingAdapter
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

## Queue Layout

Messages are stored on disk in:

- `incoming/<id>/` (staging during enqueue; atomically moved into `committed/` when complete)
- `committed/<id>/raw.eml`
- `committed/<id>/meta.json`
- `processing/<id>/` (in-flight delivery)
- `dead/<id>/dead.json` (rejected)

Retries move messages back to `committed/`. Rejects move to `dead/`.

`meta.json` includes an `attempts` counter used to enforce `max_attempts` across restarts.

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

## Testing

```bash
mix test
```

## TLS (Early)

You can enable TLS with self-signed certs for testing:

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

## Feedback

If you find problems or have suggestions, please open an issue and include:

- Repro steps
- Expected vs actual behavior
- Logs or traces if applicable
