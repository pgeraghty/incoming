# syntax=docker/dockerfile:1.7

ARG ELIXIR_IMAGE=hexpm/elixir:1.19.3-erlang-28.4-ubuntu-noble-20260210.1

FROM ${ELIXIR_IMAGE} AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV MIX_ENV=prod

COPY mix.exs mix.lock ./
COPY config config

RUN mix local.hex --force && mix local.rebar --force
RUN mix deps.get --only prod
RUN mix deps.compile

COPY lib lib

RUN mix compile
RUN mix release

FROM ubuntu:noble AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openssl \
    tini \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/incoming
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV ELIXIR_ERL_OPTIONS=+fnu

COPY --from=build /app/_build/prod/rel/incoming ./
COPY docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh && mkdir -p /var/lib/incoming /var/lib/lego/certificates

EXPOSE 2525

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
CMD ["/opt/incoming/bin/incoming", "start"]
