# Changelog

## 0.5.0

- Safety limits:
  - Queue backpressure via `queue_opts[:max_depth]` returning `421 Try again later` on overflow
  - Per-IP connection limiting via listener `max_connections_per_ip` (`421 Too many connections`)
  - Per-session `max_commands` / `max_errors` with forced disconnect (`421`)
- Telemetry:
  - Configurable `:telemetry_prefix`
  - Async dispatcher now emits `[:incoming, :delivery, :dispatch_error]` on exceptions
- Queue:
  - Disk queue runs recovery in its own init (no filesystem I/O in supervisor init)
  - TTL-based dead-letter cleanup (disk queue) via `queue_opts[:cleanup_interval_ms]` + `queue_opts[:dead_ttl_seconds]`
- Abuse protection:
  - Rate limiter ETS cap via `:rate_limit_max_entries`
- Ops:
  - Added `Incoming.Control` for stop-accepting + drain + force-close workflows

