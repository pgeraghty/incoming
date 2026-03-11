#!/bin/sh
set -eu

tls_mode="${SMTP_TLS_MODE:-disabled}"
incoming_host="${INCOMING_HOST:-localhost}"
certfile="${SMTP_TLS_CERTFILE:-/var/lib/lego/certificates/${incoming_host}.crt}"
keyfile="${SMTP_TLS_KEYFILE:-/var/lib/lego/certificates/${incoming_host}.key}"
wait_timeout="${TLS_WAIT_TIMEOUT_SECONDS:-300}"

if [ "${tls_mode}" != "disabled" ]; then
  elapsed=0
  step=5

  while [ ! -s "${certfile}" ] || [ ! -s "${keyfile}" ]; do
    if [ "${elapsed}" -ge "${wait_timeout}" ]; then
      echo "TLS is enabled but certificate files are still missing after ${wait_timeout}s."
      echo "Expected cert: ${certfile}"
      echo "Expected key: ${keyfile}"
      exit 1
    fi

    echo "Waiting for TLS certificate/key files..."
    sleep "${step}"
    elapsed=$((elapsed + step))
  done
fi

exec "$@"
