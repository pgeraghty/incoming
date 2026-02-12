defmodule Incoming do
  @moduledoc """
  Incoming is an inbound-only SMTP server library for Elixir.

  It provides:

  - SMTP listener(s) via `gen_smtp`
  - Pluggable queue backends (`Incoming.Queue.Disk`, `Incoming.Queue.Memory`)
  - A delivery adapter behaviour (`Incoming.DeliveryAdapter`)
  - A policy pipeline (`Incoming.Policy`)
  - Safety limits (backpressure, per-IP limits, session abuse limits)

  See `README.md` and `guide.md` for configuration and operational notes.
  """
end
