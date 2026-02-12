# Development Guide

## Prerequisites

- Erlang/OTP 28+
- Elixir (see version target below)
- `openssl` CLI (used by TLS tests)

Use `mise` to install the correct toolchain:

```bash
mise install
```

## Running Tests

```bash
mix test
```

All tests run sequentially (`async: false`) because they share a single SMTP listener on port 2526 and modify application env.

## Elixir Version Target

`mix.exs` specifies `elixir: "~> 1.14"`.

### CI Matrix Testing

We should test against multiple Elixir/OTP combinations to enforce the version floor. Recommended matrix:

| Elixir | OTP |
|--------|-----|
| 1.14.x | 25  |
| 1.15.x | 26  |
| 1.17.x | 27  |
| 1.19.x | 28  |

This can be done via GitHub Actions or a pre-push git hook:

```yaml
# .github/workflows/test.yml
strategy:
  matrix:
    include:
      - elixir: "1.14"
        otp: "25"
      - elixir: "1.15"
        otp: "26"
      - elixir: "1.17"
        otp: "27"
      - elixir: "1.19"
        otp: "28"
```

For local pre-push validation, a git hook could run `mix test` against the currently installed version and warn if it differs from the minimum:

```bash
# .git/hooks/pre-push (example)
#!/bin/bash
set -e
eval "$(mise activate bash)"
mix compile --warnings-as-errors
mix test
```

## Project Structure

```
lib/incoming/
  application.ex          # OTP application entry
  supervisor.ex           # Top-level supervisor
  config.ex               # Application env helpers
  validate.ex             # Config validation
  listener.ex             # gen_smtp listener setup
  session.ex              # SMTP session callbacks
  message.ex              # Message struct + header parsing
  envelope.ex             # Envelope struct
  queue.ex                # Queue behaviour
  queue/disk.ex           # Disk queue (default)
  queue/memory.ex         # In-memory queue (ETS)
  policy.ex               # Policy behaviour
  policy/pipeline.ex      # Policy chain runner
  policy/rate_limiter.ex  # Rate limiting with sliding window
  policy/rate_limiter_sweeper.ex  # Periodic ETS cleanup
  policy/hello_required.ex
  policy/max_recipients.ex
  policy/size_limit.ex
  policy/tls_required.ex
  delivery/dispatcher.ex  # Async dispatch
  delivery/supervisor.ex  # Delivery worker supervisor
  delivery/worker.ex      # Poll-based delivery worker
  delivery_adapter.ex     # Delivery adapter behaviour
  metrics.ex              # Telemetry wrapper
  id.ex                   # ID generation
```

## Architecture Notes

- **Supervision tree:** `Incoming.Supervisor` starts the queue, rate limiter sweeper, delivery supervisor, then listeners (in that order).
- **Queue abstraction:** `Incoming.Queue` is a behaviour. Swap backends via `config :incoming, queue: Incoming.Queue.Memory`.
- **Policy pipeline:** Policies run as a chain-of-responsibility at each SMTP phase. First rejection short-circuits.
- **Rate limiter:** Uses ETS with `{key, count, window_start}` tuples. The sweeper runs on a timer and bulk-deletes expired entries via `ets:select_delete`.
- **TLS modes:** `:disabled`, `:optional`, `:required` (STARTTLS), `:implicit` (connection starts in TLS). Implicit TLS passes `{:protocol, :ssl}` to gen_smtp.
- **Header parsing:** `Incoming.Message.headers/1` unfolds RFC 5322 continuation lines and joins duplicate headers with `, `. Works from both `raw_path` (disk) and `raw_data` (memory).

## Known Limitations

- SMTP DATA is fully buffered in memory by gen_smtp before being passed to the session. Streaming DATA is not yet implemented.
- The memory queue loses all messages on process/node restart.
- Rate limiter state is per-node (ETS is not distributed).

## Operational Notes

### Dead-Letter Cleanup / GC

The disk queue writes rejected messages into `dead/<id>/`. To avoid unbounded growth, a periodic GC can delete old dead-letter entries:

```elixir
config :incoming,
  queue_opts: [
    cleanup_interval_ms: 60_000,
    dead_ttl_seconds: 7 * 24 * 60 * 60
  ]
```

Set `dead_ttl_seconds: :infinity` (or `nil`) to disable TTL-based deletion.

### Graceful Shutdown / Drain

For controlled shutdowns:

1. Stop accepting new connections.
2. Drain existing sessions.
3. Force-close remaining sessions (optional).

Use:

```elixir
Incoming.Control.shutdown(timeout_ms: 5_000)
```
