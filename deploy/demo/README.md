# Public demo hosting

The always-on demo that backs the project's live dashboard. It runs the **exact
same** gateway, reconciler and sweeper as production — the only differences are
that the backing infrastructure is bundled (not managed) and the upstream is the
mock provider, so the whole thing runs on one small always-on box **at zero
cost** and with no API key to leak.

## What is exposed

Only **Grafana, read-only**. The gateway is never made public: even against the
mock it would just invite someone to spend a home server's CPU, and the
dashboard is the interesting surface anyway.

## Bring it up

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.chaos.yml \
  -f docker-compose.demo.yml \
  up -d
```

`docker-compose.demo.yml` adds `restart: unless-stopped` everywhere (so the demo
survives a reboot) and gives the mock provider a mild, realistic fault mix, so
the dashboard shows the sweeper and attributed drift instead of a flat line.

## Keep the dashboard alive

A gentle trickle of traffic, via cron (no root needed):

```bash
crontab -l 2>/dev/null | { cat; echo "*/5 * * * * $PWD/deploy/demo/trickle-load.sh"; } | crontab -
```

## Make the dashboard public

Grafana binds to `127.0.0.1:3000`. Expose it read-only with whichever tunnel you
prefer — e.g. Tailscale Funnel (`tailscale funnel 3000`, once Funnel is enabled
for the tailnet) or a Cloudflare Tunnel to `localhost:3000`. Nothing that spends
money is ever behind the tunnel.
