# CCE Scheduler Service — Flow Diagrams

All diagrams use Mermaid syntax for rendering in GitHub/IDE preview.

---

## 1. Scheduler Main Loop

The core scan-publish cycle that runs on every `fixedDelay` interval. Each instance scans **all partitions it owns** (may be multiple).

```mermaid
sequenceDiagram
    participant SL as SchedulerLoop<br/>(@Scheduled)
    participant Leader as LeaderElection
    participant Scanner as DueStepScanner
    participant DB as PostgreSQL
    participant Publisher as TransitionPublisher
    participant Kafka as Apache Kafka
    participant Metrics as Micrometer

    loop fixedDelay (5s default)
        SL->>Leader: getOwnedPartitions()
        alt No partitions owned (standby)
            SL->>Metrics: Record cycle (idle)
            Note over SL: Skip - standby mode
        else ownedPartitions = [P1, P2, ...]
            loop For each owned partition P
                SL->>Scanner: scan(batchSize, P, totalPartitions)
                Scanner->>DB: SELECT step_instance<br/>WHERE state/date thresholds met<br/>AND MOD(ABS(HASHTEXT(protocol_instance_id)), N) = P<br/>ORDER BY threshold ASC<br/>LIMIT batchSize
                DB-->>Scanner: List of StepInstance
                Scanner-->>SL: List of DueStep

                alt Steps found
                    loop For each DueStep
                        SL->>Publisher: publish(dueStep)
                        Publisher->>Kafka: Send SchedulerTriggerMessage<br/>key=protocolInstanceId
                        Kafka-->>Publisher: Ack
                        Publisher->>Metrics: publish.success++ (partition=P)
                    end
                end

                SL->>Leader: updateHeartbeat(P)
                Leader->>DB: UPDATE scheduler_lease<br/>SET last_heartbeat=now()<br/>WHERE partition_index=P
                SL->>Metrics: Record cycle (partition=P)
            end
        end
    end
```

---

## 2. Fair-Share Partitioned Leader Election Lifecycle

```mermaid
flowchart TD
    A["Service Startup"] --> B["Ensure dedicated<br/>JDBC connection"]
    B --> HB["Upsert this node's heartbeat<br/>into scheduler_node"]
    HB --> AC["activeNodes = fresh scheduler_node rows<br/>(prune stale rows)"]
    AC --> FS["fairShare = ceil(totalPartitions / activeNodes)"]
    FS --> REL{"held > fairShare?"}
    REL -->|"Yes"| RELX["pg_advisory_unlock surplus<br/>(highest-indexed first)"]
    RELX --> D
    REL -->|"No"| D{"held < fairShare<br/>and free partition i?"}
    D --> D1["sleep(random(0, lockAcquireDelayMs))<br/>// stagger simultaneous starts"]
    D1 --> D2{"pg_try_advisory_lock<br/>(lockKey + i)?"}
    D2 -->|"Acquired lock i"| E["Add i to ownedPartitions<br/>Update scheduler_lease row i"]
    E --> D
    D2 -->|"Lock held by other"| D
    D -->|"At fair share / none free"| F{"ownedPartitions<br/>empty?"}
    F -->|"Yes"| G["standby mode<br/>(still heartbeats to scheduler_node)"]
    F -->|"No"| H["leader = true<br/>owns partitions list"]

    H --> I["Start scan loop<br/>(iterates all owned partitions)"]

    G --> J["Wait leaderRetryInterval"]
    J --> HB

    I --> K{"Scan cycle<br/>completed for<br/>all owned partitions?"}
    K -->|"Yes"| L["Update heartbeat<br/>for each owned partition"]
    L --> M["Wait fixedDelay"]
    M --> HB

    subgraph Shutdown
        N["@PreDestroy"] --> O["Release held advisory locks"]
        O --> O2["Deregister from scheduler_node"]
        O2 --> P["Close dedicated connection"]
    end

    subgraph Failover
        Q["Instance dies"] --> R["PG connection drops"]
        R --> S["ALL advisory locks released;<br/>scheduler_node row goes stale & pruned"]
        S --> T["Survivors recompute larger fairShare,<br/>acquire orphaned locks on next retry"]
        T --> E
    end

    style H fill:#27AE60,color:white
    style G fill:#E67E22,color:white
    style RELX fill:#2980B9,color:white
    style S fill:#E74C3C,color:white
    style T fill:#27AE60,color:white
```

---

## 3. Due Step Scanning Algorithm

```mermaid
flowchart TD
    A["DueStepScanner.scan(batchSize, P, N)"] --> B["Query PostgreSQL"]
    B --> C["SELECT steps WHERE<br/>(PENDING AND dueDate ≤ now) OR<br/>(DUE AND overdueDate ≤ now) OR<br/>(OVERDUE AND missedDate ≤ now)<br/>AND MOD(ABS(HASHTEXT(protocol_instance_id::text)), N) = P<br/>ORDER BY threshold ASC<br/>LIMIT batchSize"]
    C --> D{"Results empty?"}
    D -->|"Yes"| E["Return empty list"]
    D -->|"No"| F["For each StepInstance"]

    F --> G{"Current state?"}
    G -->|"PENDING"| H["transitionType = PENDING_TO_DUE"]
    G -->|"DUE"| I["transitionType = DUE_TO_OVERDUE"]
    G -->|"OVERDUE"| J["transitionType = OVERDUE_TO_MISSED"]

    H --> K["Create DueStep record"]
    I --> K
    J --> K

    K --> L{"More steps?"}
    L -->|"Yes"| F
    L -->|"No"| M["Return List of DueStep"]

    style A fill:#4A90D9,color:white
    style M fill:#27AE60,color:white
```

> **P** = this instance’s `partitionIndex`, **N** = `totalPartitions`. When N=1, the `MOD(...)` clause is always 0 = P, effectively a no-op (full table scan).
### Indexing & Query Optimization Notes

**Indexes on `step_instance` (owned by Compliance Service):**

The Compliance Service defines the following indexes that benefit the Scheduler's polling query:

| Index | Columns | Purpose |
|-------|---------|--------|
| `idx_step_instance_state` | `state` WHERE `state IN ('PENDING', 'DUE', 'OVERDUE')` | Filters to only non-terminal (actionable) steps |
| `idx_step_instance_due_date` | `due_date` WHERE `state IN ('PENDING', 'DUE', 'OVERDUE')` | Scheduler time-based transitions — enables efficient range scans for threshold-crossing detection |

These **partial indexes** ensure PostgreSQL narrows down the candidate set to non-terminal steps with time thresholds *before* applying the partition filter.

**Optimization of `MOD(ABS(HASHTEXT(protocol_instance_id::text)), N) = P`:**

The hash-modulo partition filter is a non-indexable expression — it cannot leverage a B-tree index directly. However, the query is structured so that the **WHERE clause's time-based predicates** (indexed above) reduce the working set first, and the `MOD(...)` filter is applied only to the resulting rows. In practice:

1. Partial indexes reduce the scan to only rows in the matching status + time window.
2. The `MOD(HASHTEXT(...))` filter is then applied as a cheap CPU operation on the reduced set.
3. `LIMIT batchSize` caps the final output.

This keeps query cost low even without indexing the hash expression itself. If profiling shows the `MOD(...)` filter discarding too many rows (i.e., N is large and each partition gets 1/N of the pre-filtered set), consider a **functional index**:

```sql
CREATE INDEX idx_step_instance_partition ON step_instance (
    MOD(ABS(HASHTEXT(protocol_instance_id::text)), <N>)
) WHERE status IN ('PENDING', 'DUE', 'OVERDUE');
```

> **Note:** A functional index is tied to a specific value of N. If `total-partitions` changes, the index must be recreated. For most deployments (N ≤ 8), the filtered-scan approach above is sufficient without a functional index.
---

## 4. State Transition Determination

```mermaid
graph LR
    subgraph "Time-Based (Scheduler)"
        P["PENDING"] -->|"dueDate ≤ now"| D["DUE"]
        D -->|"overdueDate ≤ now"| O["OVERDUE"]
        O -->|"missedDate ≤ now"| M["MISSED"]
    end

    subgraph "Event-Based (Compliance Service)"
        P2["PENDING"] -->|"Event match"| C["COMPLETED"]
        D2["DUE"] -->|"Event match"| C2["COMPLETED"]
        O2["OVERDUE"] -->|"Event match"| C3["COMPLETED"]
        O3["OVERDUE"] -->|"Optional step<br/>auto-skip"| S["SKIPPED"]
    end

    style P fill:#3498DB,color:white
    style D fill:#F39C12,color:white
    style O fill:#E74C3C,color:white
    style M fill:#7F8C8D,color:white
    style C fill:#27AE60,color:white
    style C2 fill:#27AE60,color:white
    style C3 fill:#27AE60,color:white
    style S fill:#95A5A6,color:white
```

---

## 5. Failover Scenario (Fair-Share Partition Redistribution)

```mermaid
sequenceDiagram
    participant L1 as Instance A<br/>(Partitions 0,1)
    participant L2 as Instance B<br/>(Partition 2)
    participant PG as PostgreSQL
    participant Kafka as Kafka

    Note over L1,L2: Normal operation — 3 partitions across 2 instances
    L1->>PG: Hold advisory locks 100001, 100002
    L2->>PG: Hold advisory lock 100003
    L1->>PG: Scan partition 0, then partition 1
    L2->>PG: Scan partition 2
    L1->>Kafka: Publish transitions (partitions 0,1)
    L2->>Kafka: Publish transitions (partition 2)

    Note over L1: Instance A crashes
    L1--xPG: Connection drops
    Note over PG: Advisory locks 100001 + 100002 auto-released;<br/>A's scheduler_node row goes stale & is pruned

    Note over L2: activeNodes drops to 1 → fairShare = 3
    L2->>PG: pg_try_advisory_lock(100001)
    PG-->>L2: Lock acquired!
    L2->>PG: pg_try_advisory_lock(100002)
    PG-->>L2: Lock acquired!
    Note over L2: Now owns ALL 3 partitions

    L2->>PG: Scan partition 0, 1, 2
    L2->>Kafka: Publish transitions (all partitions)

    Note over L2: Single instance handles full table
    Note over Kafka: Compliance Service handles<br/>any duplicate messages<br/>idempotently
```
