#!/bin/bash

echo "Waiting for ClickHouse to be ready..."
until clickhouse-client --query "SELECT 1" &>/dev/null; do
  echo "ClickHouse is unavailable - sleeping"
  sleep 2
done

# Create user from environment variables
clickhouse-client --host clickhouse --query "
CREATE USER IF NOT EXISTS ${CLICKHOUSE_USER} IDENTIFIED BY '${CLICKHOUSE_PASSWORD}';
GRANT ALL ON *.* TO ${CLICKHOUSE_USER} WITH GRANT OPTION;
"

echo "Creating database and tables"
# Create database
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS logs_db"

# Create table for JSON service logs
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.json_service_logs (
    timestamp DateTime64(3),
    container_name String,
    source_type String,
    stream String,
    host String,
    method String,
    request String,
    protocol String,
    status UInt16,
    bytes UInt32,
    referer String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, container_name)
"

# Create aggregation table for JSON logs
clickhouse-client --query "
CREATE TABLE logs_db.json_1min_agg
(
    minute DateTime,
    requests UInt64,
    total_bytes UInt64,
    bytes_avg Float64
) ENGINE = SummingMergeTree()
ORDER BY minute;
"

# Create materialized view for JSON logs aggregation
clickhouse-client --query "
CREATE MATERIALIZED VIEW logs_db.json_1min_mv
TO logs_db.json_1min_agg
AS
SELECT
    toStartOfMinute(timestamp) AS minute,
    count() AS requests,
    sum(bytes) AS total_bytes,
    avg(bytes) AS bytes_avg
FROM logs_db.json_service_logs
GROUP BY minute;
"


# Create table for Apache access logs
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.apache_access_logs (
    timestamp DateTime64(3),
    container_name String,
    source_type String,
    stream String,
    host String,
    user String,
    agent String,
    method String,
    path String,
    message String,
    protocol String,
    status UInt16,
    size UInt32,
    referrer String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, container_name)
"
# Create aggregation table for Apache hits per minute
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.apache_hits_per_minute (
    ts DateTime,
    hits UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;
"
# Create table for Apache hits per minute aggregation
clickhouse-client --query "
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_db.apache_hits_per_minute_mv
TO logs_db.apache_hits_per_minute AS
SELECT
    toStartOfMinute(timestamp) AS ts,
    count() AS hits
FROM logs_db.apache_access_logs
GROUP BY ts;
"
# Create table for Apache status code per minute aggregation
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.apache_status_per_minute (
    ts DateTime,
    status UInt16,
    hits UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, status);
"
# Create materialized view for Apache status code per minute aggregation
clickhouse-client --query "
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_db.apache_status_per_minute_mv
TO logs_db.apache_status_per_minute AS
SELECT
    toStartOfMinute(timestamp) AS ts,
    status,
    count() AS hits
FROM logs_db.apache_access_logs
GROUP BY ts, status;
"
# Create table for Apache response size per minute aggregation
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.apache_response_size_per_minute (
    ts DateTime,
    avg_state AggregateFunction(avg, UInt32),
    p95_state AggregateFunction(quantile(0.95), UInt32)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;
"
# Create materialized view for Apache response size per minute aggregation
clickhouse-client --query "
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_db.apache_response_size_mv
TO logs_db.apache_response_size_per_minute AS
SELECT
    toStartOfMinute(timestamp) AS ts,
    avgState(size) AS avg_state,
    quantileState(0.95)(size) AS p95_state
FROM logs_db.apache_access_logs
GROUP BY ts;
"

# Create table for Apache error logs
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.apache_error_logs (
    timestamp DateTime64(3),
    container_name String,
    source_type String,
    stream String,
    log_timestamp String,
    module String,
    level String,
    pid UInt32,
    tid UInt32,
    client String,
    error_message String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, container_name)
"
# Create table for Apache errors per minute aggregation
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.apache_errors_per_minute (
    ts DateTime,
    error_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;
"
# Create materialized view for Apache errors per minute aggregation
clickhouse-client --query "
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_db.apache_errors_per_minute_mv
TO logs_db.apache_errors_per_minute AS
SELECT
    toStartOfMinute(timestamp) AS ts,
    count(*) AS error_count
FROM logs_db.apache_error_logs
GROUP BY ts;
"

# Create table for Apache error levels per minute aggregation
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.apache_error_levels_per_minute (
    ts DateTime,
    notice_count UInt64,
    warn_count UInt64,
    error_count UInt64,
    crit_count UInt64,
    alert_count UInt64,
    emerg_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;
"

# Create materialized view for Apache error levels per minute aggregation
clickhouse-client --query "
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_db.apache_error_levels_per_minute_mv
TO logs_db.apache_error_levels_per_minute AS
SELECT
    toStartOfMinute(timestamp) AS ts,
    countIf(level = 'notice') AS notice_count,
    countIf(level = 'warn') AS warn_count,
    countIf(level = 'error') AS error_count,
    countIf(level = 'crit') AS crit_count,
    countIf(level = 'alert') AS alert_count,
    countIf(level = 'emerg') AS emerg_count
FROM logs_db.apache_error_logs
GROUP BY ts;
"

# Create table for RFC3164 syslog logs
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.rfc3164_syslogs (
    timestamp DateTime64(3),
    container_name String,
    source_type String,
    stream String,
    hostname String,
    appname String,
    procid UInt32,
    facility String,
    severity String,
    message String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, container_name)
"

# Create table for syslogs per minute aggregation
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.syslogs_per_minute (
    ts DateTime,
    log_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;
"

# Create materialized view for syslogs per minute aggregation
clickhouse-client --query "
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_db.syslogs_per_minute_mv
TO logs_db.syslogs_per_minute AS
SELECT
    toStartOfMinute(timestamp) AS ts,
    count(*) AS log_count
FROM logs_db.rfc3164_syslogs
GROUP BY ts;
"

# Create table for syslogs severity per minute aggregation
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS logs_db.syslogs_severity_per_minute (
    ts DateTime,
    debug_count UInt64,
    info_count UInt64,
    notice_count UInt64,
    warn_count UInt64,
    err_count UInt64,
    crit_count UInt64,
    alert_count UInt64,
    emerg_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;
"

# Create materialized view for syslogs severity per minute aggregation
clickhouse-client --query "
CREATE MATERIALIZED VIEW IF NOT EXISTS logs_db.syslogs_severity_per_minute_mv
TO logs_db.syslogs_severity_per_minute AS
SELECT
    toStartOfMinute(timestamp) AS ts,
    countIf(severity='debug') AS debug_count,
    countIf(severity='info') AS info_count,
    countIf(severity='notice') AS notice_count,
    countIf(severity='warn') AS warn_count,
    countIf(severity='err') AS err_count,
    countIf(severity='crit') AS crit_count,
    countIf(severity='alert') AS alert_count,
    countIf(severity='emerg') AS emerg_count
FROM logs_db.rfc3164_syslogs
GROUP BY ts;
"

clickhouse-client --query "
CREATE VIEW logs_db.unified_logs_view AS
SELECT
    timestamp,
    container_name,
    'JSON Service' as log_type,
    toString(status) as level_or_status, -- Normalized to string
    concat(method, ' ', request) as message,
    bytes as size_bytes
FROM logs_db.json_service_logs

UNION ALL

SELECT
    timestamp,
    container_name,
    'Apache Access' as log_type,
    toString(status) as level_or_status,
    concat(method, ' ', path, ' [', message, ']') as message,
    size as size_bytes
FROM logs_db.apache_access_logs

UNION ALL

SELECT
    timestamp,
    container_name,
    'Apache Error' as log_type,
    level as level_or_status,
    error_message as message,
    0 as size_bytes
FROM logs_db.apache_error_logs

UNION ALL

SELECT
    timestamp,
    container_name,
    'Syslog' as log_type,
    severity as level_or_status,
    concat(appname, ': ', message) as message,
    0 as size_bytes
FROM logs_db.rfc3164_syslogs;
"
echo "Database and tables created successfully!"

touch /tmp/clickhouse-init-done
echo "ClickHouse initialization complete!"