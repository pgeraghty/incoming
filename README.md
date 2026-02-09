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
    Incoming.Policy.SizeLimit
  ]
```

## Feedback

If you find problems or have suggestions, please open an issue and include:

- Repro steps
- Expected vs actual behavior
- Logs or traces if applicable
