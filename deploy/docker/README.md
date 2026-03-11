# Incoming Docker Deployment (Bare Metal)

This directory is a full Docker deployment bundle for running Incoming as public SMTP ingress.

What you get:

- Single Incoming container image (`incoming` service).
- Optional Let's Encrypt automation via `lego` (`acme` service, DNS-01 challenge).
- Env-driven runtime config (`env.example` -> `.env`).

## Prerequisites

- Linux host with Docker + Docker Compose plugin.
- Public IP reachable on TCP 25.
- DNS control for your domain.
- Cloudflare API token (or other DNS provider credentials) for DNS-01.

## 1. DNS and Network Setup

Example for domain `example.com`:

- `A mail.example.com -> <server-ip>`
- `MX example.com -> 10 mail.example.com`

If you publish `AAAA`, ensure IPv6 is truly reachable.

Open firewall:

- Inbound `25/tcp` (required for SMTP delivery)

## 2. Build and Publish Image

From this directory:

```bash
INCOMING_IMAGE_REPO=pgeraghty/incoming-demo INCOMING_IMAGE_TAG=latest ./build_and_push.sh
```

Or from repo root:

```bash
docker build -t pgeraghty/incoming-demo:latest .
docker push pgeraghty/incoming-demo:latest
```

## 3. Configure Environment

Create `.env`:

```bash
cp env.example .env
```

Set at minimum:

- `INCOMING_IMAGE`
- `INCOMING_HOST` (for example `mail.example.com`)
- `INCOMING_DOMAIN` (for example `example.com`)
- `SMTP_TLS_MODE=required`
- `ACME_EMAIL`
- `ACME_DNS_PROVIDER=cloudflare`
- `CLOUDFLARE_DNS_API_TOKEN`

Notes:

- `SMTP_TLS_CERTFILE` and `SMTP_TLS_KEYFILE` should match your `INCOMING_HOST`.
- Keep DNS API tokens scoped to DNS edit for only the target zone.

## 4. Start Services

Start Incoming + ACME:

```bash
docker compose --profile acme up -d
```

Tail logs:

```bash
docker compose logs -f incoming acme
```

## 5. Verify

Basic SMTP smoke test:

```bash
swaks --server YOUR_SERVER_IP:25 --from test@sender.tld --to test@example.com
```

Check certificate files inside the shared ACME volume:

```bash
docker compose exec incoming ls -l /var/lib/lego/certificates
```

## Certificate Storage and Renewal

- Certs are persisted in Docker volume `acme-data`.
- `acme` writes cert/key into `/var/lib/lego/certificates`.
- `incoming` mounts that same path read-only.

`acme` runs a renewal loop automatically. Incoming currently reads/validates TLS certs at startup, so after renewal you should restart Incoming to guarantee new cert usage:

```bash
docker compose restart incoming
```

Recommended: run this restart on a schedule (for example nightly via cron/systemd timer).

Example cron entry:

```cron
0 3 * * * cd /opt/incoming/deploy/docker && docker compose restart incoming
```

## If You Already Manage TLS Elsewhere

Skip ACME service:

```bash
docker compose up -d incoming
```

Then set:

- `SMTP_TLS_MODE=required` (or `optional` / `implicit`)
- `SMTP_TLS_CERTFILE`
- `SMTP_TLS_KEYFILE`

## Using Host Paths Instead of Docker Named Volumes

Named volumes are default and recommended. If you want host-visible files, replace volumes in `compose.yml` with bind mounts like:

```yaml
services:
  incoming:
    volumes:
      - /srv/incoming/queue:/var/lib/incoming
      - /srv/incoming/acme:/var/lib/lego:ro
  acme:
    volumes:
      - /srv/incoming/acme:/var/lib/lego
      - ./acme/renew.sh:/scripts/renew.sh:ro
```

## Troubleshooting

- Cannot receive mail:
  - Confirm provider allows inbound TCP 25.
  - Confirm `MX` points to the same hostname configured in `INCOMING_HOST`.
  - Confirm hostname resolves to the server public IP.
- TLS startup failure:
  - Ensure cert/key exist at configured paths.
  - Check ACME logs for DNS auth failures.
