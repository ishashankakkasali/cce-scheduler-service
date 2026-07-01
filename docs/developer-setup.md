# Developer Setup & Configuration

## 1. Prerequisites

| Tool | Version | Required | Purpose |
|---|---|---|---|
| **Java JDK** | 21 LTS | Yes | Build and runtime |
| **Gradle** | 8.x | Yes | Build tool (via wrapper) |
| **Docker** | 24+ | Recommended | Run PostgreSQL, Kafka locally |
| **Docker Compose** | 2.x | Recommended | Orchestrate infrastructure |
| **PostgreSQL** | 16+ | Yes | Shared database (with Compliance Service) |
| **Apache Kafka** | 3.x | Yes | Message broker |
| **Git** | 2.x | Yes | Version control |

## 2. Quick Start

### 2.1 Clone & Build

```bash
# Clone the repository
git clone <repository-url>
cd cce-scheduler-service

# Build (skip tests for fast iteration)
./gradlew build -x test

# Build with tests
./gradlew build
```

### 2.2 Start Infrastructure

PostgreSQL, Kafka, and the shared database (`cce_collector`) are deployed by the **CCE Collector Service**. All CCE services share the same database.

```bash
# Start shared infrastructure (PostgreSQL on port 5433 + Kafka on port 9092)
cd /path/to/cce-collector-service
docker compose up -d

# Verify shared services are running
docker compose ps
```

### 2.3 Shared Database Requirement

The Scheduler Service connects to the **same PostgreSQL database** (`cce_collector`) as all other CCE services. The database and infrastructure are deployed by the **CCE Collector Service**. The `step_instance` table must exist before the Scheduler can function.

**Development options:**
1. **Run Compliance Service first** — its Flyway migrations create all tables including `step_instance`
2. **Use init script** — apply the Compliance Service schema manually before starting the Scheduler
3. **Testcontainers** — integration tests include init scripts that create both schemas

The Scheduler's own Flyway migration (`V1__create_scheduler_lease.sql`) creates only the `scheduler_lease` table.

### 2.4 Run the Application

```bash
# Run with defaults (connects to localhost:5433 and localhost:9092)
./gradlew bootRun

# Verify health
curl localhost:8083/actuator/health

# Check leader status
curl localhost:8083/actuator/health | jq '.components.leaderElection'

# Check metrics
curl localhost:8083/actuator/prometheus | grep cce_scheduler
```

## 3. Configuration Reference

### 3.1 Application Properties

```yaml
server:
  port: ${SERVER_PORT:8083}

spring:
  application:
    name: cce-scheduler-service
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5433}/${DB_NAME:cce_collector}
    username: ${DB_USERNAME:cce_user}
    password: ${DB_PASSWORD:cce_pass}
    hikari:
      maximum-pool-size: ${DB_POOL_SIZE:5}
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
    properties:
      hibernate:
        jdbc:
          time_zone: UTC
  flyway:
    enabled: true
    locations: classpath:db/migration
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      acks: all
      retries: 3
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5

cce:
  scheduler:
    scan-interval: ${SCHEDULER_SCAN_INTERVAL:5000}
    batch-size: ${SCHEDULER_BATCH_SIZE:100}
    lease-duration-seconds: ${SCHEDULER_LEASE_DURATION:30}
    leader-retry-interval: ${SCHEDULER_LEADER_RETRY:5000}
    advisory-lock-key: ${SCHEDULER_LOCK_KEY:100001}
    total-partitions: ${SCHEDULER_TOTAL_PARTITIONS:1}
    lock-acquire-delay-ms: ${SCHEDULER_LOCK_ACQUIRE_DELAY:50}
  kafka:
    topics:
      scheduler-triggers: ${KAFKA_TOPIC_SCHEDULER_TRIGGERS:cce.scheduler.triggers}

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      show-details: always
      probes:
        enabled: true
  metrics:
    tags:
      application: cce-scheduler-service
```

### 3.2 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8083` | HTTP port (actuator only) |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5433` | PostgreSQL port (shared with Collector Service) |
| `DB_NAME` | `cce_collector` | Shared database name (all CCE services) |
| `DB_USERNAME` | `cce_user` | Database username (shared with Collector Service) |
| `DB_PASSWORD` | `cce_pass` | Database password (shared with Collector Service) |
| `DB_POOL_SIZE` | `5` | HikariCP max pool size |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC_SCHEDULER_TRIGGERS` | `cce.scheduler.triggers` | Output Kafka topic |
| `SCHEDULER_SCAN_INTERVAL` | `5000` | Scan interval in milliseconds |
| `SCHEDULER_BATCH_SIZE` | `100` | Max steps per scan cycle |
| `SCHEDULER_LEASE_DURATION` | `30` | Lease expiry in seconds |
| `SCHEDULER_LEADER_RETRY` | `5000` | Leader retry interval in milliseconds |
| `SCHEDULER_LOCK_KEY` | `100001` | Base PostgreSQL advisory lock key. Partitions use keys `LOCK_KEY + 0` through `LOCK_KEY + TOTAL_PARTITIONS - 1`. |
| `SCHEDULER_TOTAL_PARTITIONS` | `1` | Number of scan partitions for horizontal scaling. `1` = single-leader (default). |

## 4. Project Structure

```
cce-scheduler-service/
├── build.gradle
├── settings.gradle
├── gradlew / gradlew.bat
├── gradle/wrapper/
├── docker-compose.yml
├── Dockerfile
├── .gitignore
├── README.md
├── docs/
│   ├── architecture-overview.md
│   ├── data-dictionary.md
│   ├── developer-setup.md
│   ├── kafka-events.md
│   └── flow-diagrams.md
└── src/
    ├── main/
    │   ├── java/org/openphc/cce/scheduler/
    │   │   ├── SchedulerServiceApplication.java
    │   │   ├── config/
    │   │   ├── domain/model/ + domain/model/enums/ + domain/repository/
    │   │   ├── engine/
    │   │   ├── health/
    │   │   ├── kafka/
    │   │   └── leader/
    │   └── resources/
    │       ├── application.yml
    │       └── db/migration/V1__create_scheduler_lease.sql
    ├── test/java/                    # Unit tests
    └── integrationTest/java/         # Integration tests (Testcontainers)
```

## 5. Testing

### 5.1 Commands

```bash
# Unit tests
./gradlew test

# Integration tests
./gradlew integrationTest

# All tests
./gradlew build

# Specific test
./gradlew test --tests SchedulerLoopTest

# Coverage report
./gradlew test jacocoTestReport
```

### 5.2 Test Infrastructure

Integration tests use **Testcontainers** for PostgreSQL and Kafka. Test init scripts create both the Compliance Service schema (`step_instance`, `protocol_instance`) and the Scheduler schema (`scheduler_lease`).

No mock databases are used — all integration tests run against real PostgreSQL and Kafka instances inside Docker containers.

## 6. Docker Build

```dockerfile
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app
COPY gradle/ gradle/
COPY gradlew build.gradle settings.gradle ./
RUN ./gradlew dependencies --no-daemon
COPY src/ src/
RUN ./gradlew build -x test --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
```

```bash
# Build Docker image
docker build -t cce-scheduler-service .

# Run with Docker
docker run -p 8083:8083 \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5433 \
  -e DB_NAME=cce_collector \
  -e DB_USERNAME=cce_user \
  -e DB_PASSWORD=cce_pass \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e SCHEDULER_TOTAL_PARTITIONS=1 \
  cce-scheduler-service
```
