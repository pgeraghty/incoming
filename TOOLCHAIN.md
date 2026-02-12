# Toolchain / Releases

This repo is sensitive to mismatched Elixir/Mix/Hex toolchains.

If you see strange `mix hex.*` crashes (for example around `Regex` or "Local password"),
assume you're running the wrong toolchain first.

## Source Of Truth

We use `mise` to manage Elixir/Erlang.

- Elixir: `1.19.5` (OTP 28)
- Erlang/OTP: `28.x`

Check what you are actually using:

```bash
elixir -v
mix -v
mise current
```

## Setup

Install the toolchain:

```bash
mise install
```

Activate mise in your shell (pick one):

```bash
eval "$(mise activate bash)"
```

or

```bash
eval "$(mise activate zsh)"
```

## Publishing To Hex / HexDocs

Publishing requires an API key.

Recommended (avoids interactive prompts):

```bash
export HEX_API_KEY=...
mix local.hex --force
mix local.rebar --force
mix hex.publish --yes
```

Notes:

- `mix hex.publish` publishes the package and docs. HexDocs updates when the publish succeeds.
- If you run `mix hex.publish docs`, Hex still needs auth; use `HEX_API_KEY` to avoid the "Local password" prompt.

