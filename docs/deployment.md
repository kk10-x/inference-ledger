# Running this on a home server

Notes for hosting the stack on a small always-on box rather than a laptop.

## This service does not get a public URL

Unlike a normal web app, the gateway holds a **provider API key and spends money
on every request it accepts**. Putting it behind a public reverse proxy or a
Cloudflare Tunnel means anyone who finds the hostname can bill charges to that
key. Per-tenant budgets cap the damage; they do not prevent it, and a tenant
header is not authentication.

Every port in `docker-compose.yml` is therefore bound to `127.0.0.1`. Reach them
from a workstation with an SSH tunnel:

```bash
ssh -N -L 8080:localhost:8080 -L 3000:localhost:3000 khrithik@bella
```

Grafana is then at `http://localhost:3000` and the gateway at
`http://localhost:8080`, with nothing listening on the LAN.

If a public demo is ever genuinely needed, expose **Grafana only**, read-only,
behind the tunnel's own access control — never the gateway.

## Resource notes

Sized against a 16GB / 12-thread host with ~39GB free disk.

| Concern | Setting | Why |
|---|---|---|
| Redpanda memory | `--memory 1G --reserve-memory 0M` | Redpanda claims most of the host's RAM unless capped |
| Redpanda cores | `--smp 1 --overprovisioned` | Disables real-time scheduling assumptions on a shared box |
| Image footprint | ~3GB total | Run `docker system prune -a` after rebuilds; the gateway image is rebuilt often |
| Postgres/Redis volumes | ephemeral by default | `make down` uses `-v`; add named volumes before keeping data across restarts |

The GPU is irrelevant here — this project serves a hosted provider's API and runs
no local model. Nothing in the stack will touch it.

## Keeping the chaos suite honest

`make chaos` deliberately kills containers, pauses the broker and forces consumer
rebalances. Run it against a dedicated compose project, not alongside anything
you care about staying up.
