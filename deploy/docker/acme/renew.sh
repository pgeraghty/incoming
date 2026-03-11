#!/bin/sh
set -eu

: "${ACME_EMAIL:?ACME_EMAIL is required}"
: "${ACME_DNS_PROVIDER:?ACME_DNS_PROVIDER is required}"
: "${INCOMING_HOST:?INCOMING_HOST is required}"

acme_path="${ACME_PATH:-/var/lib/lego}"
renew_days="${ACME_RENEW_DAYS:-30}"
renew_interval_seconds="${ACME_RENEW_INTERVAL_SECONDS:-43200}"
cert_file="${acme_path}/certificates/${INCOMING_HOST}.crt"
key_file="${acme_path}/certificates/${INCOMING_HOST}.key"

lego_cmd() {
  lego_bin="${LEGO_BIN:-}"
  if [ -z "${lego_bin}" ]; then
    if command -v lego >/dev/null 2>&1; then
      lego_bin="lego"
    elif [ -x /lego ]; then
      lego_bin="/lego"
    else
      echo "Could not find lego binary (tried PATH and /lego)" >&2
      exit 1
    fi
  fi

  "${lego_bin}" \
    --accept-tos \
    --email "${ACME_EMAIL}" \
    --dns "${ACME_DNS_PROVIDER}" \
    --domains "${INCOMING_HOST}" \
    --path "${acme_path}" \
    "$@"
}

if [ ! -s "${cert_file}" ] || [ ! -s "${key_file}" ]; then
  echo "Issuing initial certificate for ${INCOMING_HOST}"
  lego_cmd run
else
  echo "Existing certificate found for ${INCOMING_HOST}; skipping initial issue"
fi

while true; do
  echo "Checking certificate renewal eligibility for ${INCOMING_HOST}"
  lego_cmd renew --days "${renew_days}" || true
  sleep "${renew_interval_seconds}"
done
