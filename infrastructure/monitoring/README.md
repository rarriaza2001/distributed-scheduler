# Monitoring

Grafana dashboard **templates** (import into your Grafana instance and bind your Prometheus datasource):

- [`grafana/`](grafana/) — JSON dashboards for scheduler health, queue pressure, workers, retries, latency, leadership.

These files are not executed by the Go service; they are operator-facing UI templates.
