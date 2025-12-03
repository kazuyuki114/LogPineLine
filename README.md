# Log Pipeline

A complete log aggregation and visualization pipeline using Docker containers.

## Architecture

This pipeline collects, parses, stores, and visualizes logs from multiple sources:

- **Log Sources** - Generates sample logs (JSON, Apache, Syslog formats)
- **Log Shipping** - Collects logs using Vector
- **Kafka** - Message broker for log streaming
- **Log Parsing** - Parses and transforms logs using Vector
- **ClickHouse** - Time-series database for log storage
- **Grafana** - Dashboard visualization
- **Prometheus** - Metrics collection

## Supported Log Formats

- JSON service logs
- Apache access logs (Combined format)
- Apache error logs
- RFC3164 Syslog

## Quick Start

```bash
# Start all services
docker-compose up -d

# Or start individual components
docker-compose -f kafka/docker-compose.yml up -d
docker-compose -f clickhouse/docker-compose.yml up -d
docker-compose -f log_sources/docker-compose.yml up -d
docker-compose -f log_shipping/docker-compose.yml up -d
docker-compose -f log_parsing/docker-compose.yml up -d
docker-compose -f grafana/docker-compose.yml up -d
docker-compose -f prometheus/docker-compose.yml up -d
```

## Dashboards

Pre-configured Grafana dashboards are available in the `dashboard/` folder:

- `ApacheCombinedDashboard.json` - Apache access log metrics
- `ApacheErrorDashboard.json` - Apache error log metrics
- `SyslogDashboard.json` - Syslog metrics
- `JsonDashboard.json` - JSON service log metrics
- `CombinedDashboard.json` - Unified view of all logs
- `Kafka.json` - Kafka metrics
- `Clickhouse.json` - ClickHouse metrics

## Project Structure

```
├── clickhouse/          # ClickHouse database setup
├── dashboard/           # Grafana dashboard JSON files
├── grafana/             # Grafana configuration
├── kafka/               # Kafka message broker setup
├── log_parsing/         # Vector log parsing configuration
├── log_shipping/        # Vector log shipping configuration
├── log_sources/         # Sample log generators
├── prometheus/          # Prometheus metrics configuration
└── docker-compose.yml   # Main orchestration file
```

## License
MIT
