# CCE Scheduler Service — Deployment Guide

## Prerequisites

| Component    | Version  | Notes                                    |
|------------- |----------|------------------------------------------|
| Java         | 21 LTS   | Eclipse Temurin recommended              |
| PostgreSQL   | 16+      | Shared `ccedb` database          |
| Apache Kafka | 3.6+     | Topic auto-create or pre-provisioned     |
| Docker       | 24+      | For container deployments                |
| Kubernetes   | 1.28+    | For orchestrated deployments (optional)  |

---

## 1. Configuration Reference

All configuration is via environment variables. Defaults are suitable for local development.

### Core Settings

| Variable                        | Default                                         | Description                              |
|---------------------------------|-------------------------------------------------|------------------------------------------|
| `SERVER_PORT`                   | `8083`                                          | HTTP port (actuator only)                |
| `DB_URL`                        | `jdbc:postgresql://localhost:5432/ccedb` | PostgreSQL JDBC URL                      |
| `DB_USERNAME`                   | `postgres`                                      | Database username                        |
| `DB_PASSWORD`                   | `postgres`                                      | Database password                        |
| `KAFKA_BOOTSTRAP_SERVERS`       | `localhost:9092`                                | Kafka broker addresses                   |
| `KAFKA_TOPIC_SCHEDULER_TRIGGERS`| `cce.scheduler.triggers`                        | Output topic name                        |

### Scheduler Tuning

| Variable                        | Default   | Description                                          |
|---------------------------------|-----------|------------------------------------------------------|
| `SCHEDULER_SCAN_INTERVAL`       | `5000`    | Polling interval in ms (fixedDelay)                  |
| `SCHEDULER_BATCH_SIZE`          | `100`     | Max steps fetched per scan cycle                     |
| `SCHEDULER_LEASE_DURATION`      | `30`      | Leader lease TTL in seconds                          |
| `SCHEDULER_LEADER_RETRY`        | `5000`    | Retry interval for leader acquisition (ms)           |
| `SCHEDULER_LOCK_KEY`            | `100001`  | Base pg_advisory_lock key                            |
| `SCHEDULER_TOTAL_PARTITIONS`    | `1`       | Number of partitions for multi-leader mode           |
| `SCHEDULER_LOCK_ACQUIRE_DELAY`  | `50`      | Jitter between lock attempts (ms); even split is enforced by the fair-share cap, not this |

---

## 2. Database Setup

The service uses Flyway for automatic schema migration. On first startup it will create the `scheduler_lease` and `scheduler_node` tables in the shared `ccedb` database.

**Pre-requisites:**
- The `ccedb` database must exist
- The `step_instance` table (managed by Compliance Service) must exist
- The service user needs `SELECT` on `step_instance` and full access to `scheduler_lease` and `scheduler_node`

```sql
-- Minimal grants (if not using superuser)
GRANT SELECT ON step_instance TO scheduler_user;
GRANT ALL ON scheduler_lease TO scheduler_user;
GRANT ALL ON scheduler_node TO scheduler_user;
GRANT USAGE ON SCHEMA public TO scheduler_user;
```

---

## 3. Kafka Topic Setup

If auto-create is disabled, pre-create the topic:

```bash
kafka-topics --bootstrap-server <brokers> \
  --create \
  --topic cce.scheduler.triggers \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2
```

---

## 4. Running Locally

### With Docker Compose

```bash
docker compose up -d
```

This starts PostgreSQL, Kafka (KRaft mode), and the scheduler service. The service will be available at `http://localhost:8083/actuator/health`.

### Without Docker

```bash
# Build
./gradlew bootJar

# Run
java -jar build/libs/cce-scheduler-service-1.0.0-SNAPSHOT.jar \
  --spring.datasource.url=jdbc:postgresql://localhost:5432/ccedb
```

---

## 5. Docker Image

### Build

```bash
docker build -t openphc/cce-scheduler-service:1.0.0 .
```

### Run

```bash
docker run -d \
  --name cce-scheduler \
  -e DB_URL=jdbc:postgresql://db-host:5432/ccedb \
  -e DB_USERNAME=scheduler_user \
  -e DB_PASSWORD=<secret> \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka-host:9092 \
  -e SCHEDULER_TOTAL_PARTITIONS=4 \
  -p 8083:8083 \
  openphc/cce-scheduler-service:1.0.0
```

### JVM Tuning

The Dockerfile uses ZGC with 75% max RAM. Override with:

```bash
docker run -d \
  -e JAVA_TOOL_OPTIONS="-XX:+UseG1GC -Xmx512m" \
  openphc/cce-scheduler-service:1.0.0
```

---

## 6. Kubernetes Deployment

Apply manifests from the `deploy/k8s/` directory:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl create secret generic scheduler-db-credentials \
  --namespace cce \
  --from-literal=DB_URL=jdbc:postgresql://pg-host:5432/ccedb \
  --from-literal=DB_USERNAME=scheduler_user \
  --from-literal=DB_PASSWORD=<secret>
kubectl apply -f deploy/k8s/
```

### Scaling

- **Horizontal scaling**: Deploy N replicas. Each replica competes for partition locks using `pg_advisory_lock`, bounded to its fair share (`ceil(SCHEDULER_TOTAL_PARTITIONS / live-replicas)`). Set `SCHEDULER_TOTAL_PARTITIONS` ≥ replica count to give every replica work.
- **Recommended**: `replicas = SCHEDULER_TOTAL_PARTITIONS` for one partition per replica.
- **Self-rebalancing**: when a replica is added, over-provisioned replicas shed their surplus partitions within a few `SCHEDULER_LEADER_RETRY` cycles (≈5–10s) — no rolling restart needed. This requires the `scheduler_node` registry; all replicas must share the same database.

### Resource Recommendations

| Workload | CPU Request | CPU Limit | Memory Request | Memory Limit |
|----------|-------------|-----------|----------------|--------------|
| Low      | 100m        | 500m      | 256Mi          | 512Mi        |
| Medium   | 250m        | 1000m     | 512Mi          | 1Gi          |
| High     | 500m        | 2000m     | 1Gi            | 2Gi          |

---

## 7. Health & Readiness

| Endpoint                            | Purpose                              |
|-------------------------------------|--------------------------------------|
| `/actuator/health/liveness`         | Liveness probe — app is running      |
| `/actuator/health/readiness`        | Readiness probe — DB + Kafka ready   |
| `/actuator/health`                  | Full health details                  |
| `/actuator/prometheus`              | Prometheus metrics scrape endpoint   |

---

## 8. Observability

### Prometheus Metrics

| Metric                                    | Type    | Labels                   | Description                       |
|-------------------------------------------|---------|--------------------------|-----------------------------------|
| `scheduler.scan.duration`                 | Timer   | partition                | Scan cycle duration               |
| `scheduler.scan.steps`                    | Counter | transitionType, partition| Steps found per scan              |
| `scheduler.publish.success`               | Counter | transitionType, partition| Successfully published events     |
| `scheduler.publish.failure`               | Counter | transitionType, partition| Failed publish attempts           |
| `scheduler.cycle`                         | Counter | partition                | Total scheduler cycles            |
| `leader.status`                           | Gauge   | partition                | 1 = leader, 0 = follower          |

### Grafana Dashboard

Import the metrics above into Grafana. Key panels:
- Scan rate and duration (P50/P99)
- Publish success vs failure ratio
- Leader partition ownership heatmap
- HikariCP connection pool utilization

### Logging

Structured JSON logging. Key MDC fields:
- `partition` — owned partition index
- `correlationId` — per-event trace ID (`sched-{type}-{prefix}-{timestamp}`)

---

## 9. Security Checklist

- [ ] Database credentials stored in secrets manager (Vault, K8s secrets, AWS Secrets Manager)
- [ ] Kafka mTLS or SASL enabled for production
- [ ] Actuator endpoints restricted to internal network / service mesh
- [ ] Container runs as non-root user (`scheduler`)
- [ ] Network policies restrict egress to DB + Kafka only
- [ ] Image scanned for CVEs before deployment

---

## 10. Troubleshooting

| Symptom                           | Likely Cause                        | Fix                                       |
|-----------------------------------|-------------------------------------|-------------------------------------------|
| No steps being published          | Leader election failed              | Check DB connectivity, advisory lock key  |
| Duplicate events                  | Multiple instances same partition   | Verify `SCHEDULER_TOTAL_PARTITIONS`       |
| High scan duration                | Large `step_instance` table         | Add indexes, reduce `BATCH_SIZE`          |
| Kafka timeout errors              | Broker unreachable                  | Check `KAFKA_BOOTSTRAP_SERVERS`           |
| Lease expired warnings            | Long GC pauses or DB latency        | Increase `SCHEDULER_LEASE_DURATION`       |
| OOM killed                        | Memory limit too low                | Increase pod memory limit                 |
