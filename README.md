# Log Pipeline

A complete log aggregation and visualization pipeline using Docker containers.

## Architecture

This pipeline collects, parses, stores, and visualizes logs from multiple sources:

- **Log Sources** - Generates sample logs (JSON, Apache, Syslog formats)
- **Log Shipping** - Collects logs using Vector
- **Kafka** - Message broker for log streaming (KRaft mode)
- **Log Parsing** - Parses and transforms logs using Vector
- **ClickHouse** - Time-series database for log storage
- **Grafana** - Dashboard visualization
- **Prometheus** - Metrics collection
- **MCP Servers** - Manage ClickHouse and Grafana configurations

## Supported Log Formats

- JSON service logs
- Apache access logs (Combined format)
- Apache error logs
- RFC3164 Syslog

## Quick Start

```bash
# Start all services
docker compose up -d

# Or start individual components
docker compose -f kafka/docker-compose.yml up -d
docker compose -f clickhouse/docker-compose.yml up -d
docker compose -f log_sources/docker-compose.yml up -d
docker compose -f log_shipping/docker-compose.yml up -d
docker compose -f log_parsing/docker-compose.yml up -d
docker compose -f grafana/docker-compose.yml up -d
docker compose -f prometheus/docker-compose.yml up -d
```

## Health Checks

The pipeline includes health checks for critical services:

### Kafka
- **Test**: `kafka-broker-api-versions --bootstrap-server localhost:9092`
- **Interval**: 30s
- **Timeout**: 10s
- **Retries**: 5
- **Start Period**: 30s

### ClickHouse
- **Test**: HTTP ping on `/ping` endpoint (port 8123)
- **Interval**: 30s
- **Timeout**: 10s
- **Retries**: 5
- **Start Period**: 30s

Dependent services wait for these health checks to pass before starting, ensuring proper initialization order.

## Service Ports

| Service | Port | Description |
|---------|------|-------------|
| Kafka | 9092 | Kafka broker |
| Kafka UI | 8080 | Kafka management UI |
| ClickHouse | 8123 | HTTP interface |
| ClickHouse | 9000 | Native client |
| Grafana | 3000 | Dashboard UI |
| Prometheus | 9090 | Metrics UI |
| Kafka Exporter | 9308 | Kafka metrics |
| ClickHouse Exporter | 9116 | ClickHouse metrics |

## Dashboards

Pre-configured Grafana dashboards are available in the `dashboard/` folder:

- `ApacheCombinedDashboard.json` - Apache access log metrics
- `ApacheErrorDashboard.json` - Apache error log metrics
- `SyslogDashboard.json` - Syslog metrics
- `JsonDashboard.json` - JSON service log metrics
- `CombinedDashboard.json` - Unified view of all logs

## Project Structure

```
├── clickhouse/          # ClickHouse database setup
├── clickhouse_mcp/      # ClickHouse MCP server
├── dashboard/           # Grafana dashboard JSON files
├── grafana/             # Grafana configuration
├── grafana_mcp/         # Grafana MCP server
├── kafka/               # Kafka message broker setup
├── log_parsing/         # Vector log parsing configuration
├── log_shipping/        # Vector log shipping configuration
├── log_sources/         # Sample log generators
├── n8n/                 # N8N workflow automation
├── prometheus/          # Prometheus metrics configuration
└── docker-compose.yml   # Main orchestration file
```

## Stopping Services

```bash
# Stop all services
docker compose down

# Stop and remove volumes
docker compose down -v
```

## License

MIT
