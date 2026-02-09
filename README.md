***CAUTION: AI VIBE CODED PRODUCT***
***EXPERIMENTAL - WORK IN PROGRESS***

Please report weaknesses, flaws, edge cases, or unexpected behavior via GitHub Issues or Discussions.

# Incoming

Production-grade inbound SMTP server library for Elixir.

## Status

This is an early placeholder package to reserve the name on Hex. The implementation is in progress.

## Installation

Add `incoming` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:incoming, "~> 0.1.0"}
  ]
end
```

## Usage

```elixir
# Placeholder API — subject to change
Incoming.hello()
```

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

- `committed/<id>/raw.eml`
- `committed/<id>/meta.json`
- `processing/<id>/` (in-flight delivery)
- `dead/<id>/dead.json` (rejected)

Retries move messages back to `committed/`. Rejects move to `dead/`.

## Telemetry (Early)

Events emitted:

- `[:incoming, :message, :queued]` measurements: `%{count: 1}`, metadata: `%{id, size, queue_depth}`
- `[:incoming, :delivery, :result]` measurements: `%{count: 1}`, metadata: `%{id, outcome, reason}`
- `[:incoming, :session, :connect]` measurements: `%{count: 1}`, metadata: `%{peer}`
- `[:incoming, :session, :accepted]` measurements: `%{count: 1}`, metadata: `%{id}`
- `[:incoming, :session, :rejected]` measurements: `%{count: 1}`, metadata: `%{reason}`
- `[:incoming, :queue, :depth]` measurements: `%{count}`, metadata: `%{}`

## Delivery Adapter (Early)

Implement `Incoming.DeliveryAdapter` and configure it:

```elixir
config :incoming, delivery: MyApp.IncomingAdapter
```

Return values:

- `:ok` -> message acked
- `{:retry, reason}` -> message requeued with backoff
- `{:reject, reason}` -> message moved to dead-letter

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
