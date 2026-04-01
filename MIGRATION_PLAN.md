# Pentaho-to-Java Migration Plan

## 1. Problem Statement

Migrate **1,500 Pentaho jobs/transformations** (.kjb/.ktr) to a Java-based orchestration and transformation engine built on **Spring Reactor WebFlux (Mono/Flux)**.

### Why Migrate?

- Pentaho's visual ETL designer doesn't scale well for version control, testing, and CI/CD
- XML-based job/transformation definitions are fragile and hard to refactor
- Limited observability, retry semantics, and backpressure handling
- Licensing and operational costs
- Reactive stack enables non-blocking, high-throughput data pipelines

---

## 2. Pentaho Concepts → Java Mapping

| Pentaho Concept | Java/Reactor Equivalent |
|---|---|
| **Job (.kjb)** | `Mono<Void>` orchestration chain (a sequence of steps) |
| **Transformation (.ktr)** | `Flux<Row>` data processing pipeline |
| **Job Entry (step)** | A `StepExecutor` bean returning `Mono<StepResult>` |
| **Hop (OK/Error/Unconditional)** | `.flatMap()` / `.onErrorResume()` / `.then()` |
| **Transformation Step** | A `RowTransformer` operator in a Flux pipeline |
| **Row** | A `DataRow` (Map-like or strongly-typed POJO) |
| **Variables / Parameters** | `JobContext` (reactive context / immutable map) |
| **Sub-job** | Nested `Mono<StepResult>` composition |
| **Sub-transformation** | Nested `Flux<DataRow>` composition |
| **Parallel execution** | `Flux.merge()` / `Mono.zip()` / `parallel().runOn()` |
| **Sequential execution** | `Mono.then()` / `.flatMap()` chaining |
| **Conditional hop** | `.filter()` / `.switchIfEmpty()` / `Mono.defer()` |
| **Error handling** | `.onErrorResume()` / `.retry()` / `.onErrorMap()` |
| **Logging** | MDC-aware reactive logging via `contextWrite()` |
| **Database connection** | R2DBC `ConnectionFactory` (reactive) or JDBC via `Schedulers.boundedElastic()` |
| **File I/O** | `Flux<DataBuffer>` / reactive file reading |
| **REST calls** | `WebClient` returning `Mono<T>` / `Flux<T>` |

---

## 3. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        API / Trigger Layer                       │
│  (REST endpoints, Scheduler/Cron, Kafka consumers, File watch)  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Job Orchestrator Engine                      │
│                                                                 │
│  JobDefinition ──► builds Mono<Void> chain from step graph      │
│  Handles: sequencing, parallelism, conditionals, error routing  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Step Executor│ │ Step Executor│ │ Step Executor│
│  (DB Query)  │ │ (REST Call)  │ │ (Transform)  │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │
       ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Transformation Engine                           │
│                                                                 │
│  Flux<DataRow> pipeline: read → transform → transform → write   │
│  Handles: row-level ops, lookups, aggregations, splits, joins   │
└─────────────────────────────────────────────────────────────────┘
       │                │                │
       ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Connector Layer                               │
│  R2DBC / JDBC / WebClient / S3 / SFTP / Kafka / File I/O       │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Core Module Structure

```
pentaho-to-java/
├── pom.xml (parent)
├── migration-engine/                    # Automation: parses Pentaho XML → Java
│   ├── pentaho-parser/                  # Parses .kjb/.ktr XML into IR
│   ├── code-generator/                  # IR → Java source code generation
│   └── validation/                      # Validates generated code vs original
│
├── orchestration-engine/                # Runtime engine
│   ├── core/                            # Core abstractions
│   │   ├── model/                       # JobDefinition, StepDefinition, DataRow
│   │   ├── engine/                      # JobOrchestrator, StepExecutor SPI
│   │   ├── context/                     # JobContext, variable resolution
│   │   └── error/                       # Error handling, retry policies
│   │
│   ├── steps/                           # Built-in step implementations
│   │   ├── db/                          # TableInput, TableOutput, DBLookup
│   │   ├── file/                        # CsvFileInput (streaming), ExcelInput, FileOutput
│   │   ├── transform/                   # SelectValues, Calculator, ScriptStep
│   │   ├── blocking/                    # ExternalSort, StreamingGroupBy, StreamingDedupe
│   │   ├── flow/                        # Switch/Case, Filter, Abort, Dummy
│   │   ├── rest/                        # HttpClient step, REST input
│   │   ├── messaging/                   # Kafka produce/consume, JMS
│   │   └── scripting/                   # Groovy/JS script step (escape hatch)
│   │
│   ├── connectors/                      # Connection management
│   │   ├── r2dbc/                       # Reactive DB connections
│   │   ├── jdbc/                        # Blocking DB (wrapped in elastic scheduler)
│   │   ├── sftp/                        # SFTP connector
│   │   └── s3/                          # S3 connector
│   │
│   └── observability/                   # Monitoring & tracing
│       ├── metrics/                     # Micrometer metrics per step/job
│       ├── tracing/                     # Distributed tracing (OpenTelemetry)
│       └── logging/                     # Structured logging with MDC context
│
├── generated-jobs/                      # Auto-generated Java jobs (output of migration)
│   ├── domain-a/
│   ├── domain-b/
│   └── ...
│
├── admin-api/                           # Spring WebFlux REST API
│   ├── job-management/                  # CRUD, trigger, schedule jobs
│   ├── monitoring/                      # Job run history, dashboards
│   └── config/                          # Connection configs, variables
│
└── tests/
    ├── integration/                     # End-to-end pipeline tests
    ├── parity/                          # Output comparison: Pentaho vs Java
    └── performance/                     # Throughput & latency benchmarks
```

---

## 5. Core Abstractions (Design)

### 5.1 DataRow

```java
public interface DataRow {
    Object get(String field);
    DataRow with(String field, Object value);
    DataRow without(String field);
    Map<String, Object> toMap();
    Set<String> fieldNames();
}
```

### 5.2 StepExecutor (SPI for Job Steps)

```java
public interface StepExecutor {
    /** Execute one step in a job, returning a result. */
    Mono<StepResult> execute(JobContext context);
}

public record StepResult(
    StepStatus status,       // SUCCESS, FAILURE, SKIPPED
    JobContext context,       // Potentially enriched context
    Map<String, Object> outputs
) {}
```

### 5.3 RowTransformer (SPI for Transformation Steps)

```java
public interface RowTransformer {
    /** Transform a stream of rows. */
    Flux<DataRow> apply(Flux<DataRow> input, TransformContext context);
}
```

### 5.4 JobOrchestrator (Builds the Reactive Chain)

```java
public class JobOrchestrator {

    public Mono<JobResult> execute(JobDefinition job, JobContext context) {
        // Walks the step graph in topological order
        // Builds a Mono chain: step1.then(step2).then(parallel(step3, step4)).then(step5)
        // Wires error hops to onErrorResume
        // Returns final Mono<JobResult>
    }
}
```

### 5.5 TransformationPipeline (Builds Flux Chains)

```java
public class TransformationPipeline {

    public Flux<DataRow> execute(TransformationDefinition def, TransformContext ctx) {
        // source step → Flux<DataRow>
        // chain of RowTransformers via .transform()
        // sink step → write output
    }
}
```

---

## 6. Automation Engine: Pentaho XML → Java Code

This is the key accelerator for migrating 1,500 jobs without hand-coding each one.

### 6.1 Pipeline

```
.kjb/.ktr files
      │
      ▼
┌──────────────┐    ┌────────────────┐    ┌──────────────────┐
│ Pentaho XML  │───►│ Intermediate   │───►│ Java Source Code  │
│ Parser       │    │ Representation │    │ Generator         │
└──────────────┘    │ (IR)           │    └──────────────────┘
                    └────────────────┘           │
                                                 ▼
                                        ┌──────────────────┐
                                        │ Compile & Validate│
                                        └──────────────────┘
```

### 6.2 Intermediate Representation (IR)

```java
public record JobIR(
    String name,
    Map<String, String> parameters,
    List<StepIR> steps,
    List<HopIR> hops    // directed edges: from → to, type (OK/ERROR/UNCONDITIONAL)
) {}

public record StepIR(
    String id,
    String type,                    // e.g., "TABLE_INPUT", "HTTP_CLIENT"
    Map<String, Object> config      // step-specific configuration
) {}

public record HopIR(String from, String to, HopType type) {}
```

### 6.3 Code Generator Strategy

For each Pentaho step type, we need a **CodeGenTemplate**:

| Pentaho Step Type | Code Generator Produces |
|---|---|
| `START` | Entry point of Mono chain |
| `TABLE_INPUT` | `Flux<DataRow>` from R2DBC query |
| `TABLE_OUTPUT` | `.flatMap(row -> r2dbcInsert(row))` |
| `SELECT_VALUES` | `.map(row -> row.select("col1","col2"))` |
| `FILTER_ROWS` | `.filter(row -> condition)` |
| `SWITCH_CASE` | `.groupBy(row -> classify(row))` |
| `HTTP_CLIENT` | `webClient.get()...` |
| `SCRIPT` | Inline Groovy/JS eval (escape hatch) |
| `SORT_ROWS` | `ExternalSortOperator` (disk-backed, only true blocking step) |
| `GROUP_BY` | `StreamingGroupByOperator` — `.bufferUntilChanged()` on sorted input |
| `UNIQUE_ROWS` | `StreamingDedupeOperator` — `.distinctUntilChanged()` on sorted input |
| `MERGE_JOIN` | `StreamingMergeJoinOperator` — two sorted streams, pointer merge |
| `SUCCESS` | `.then()` terminal |
| `MAIL` | Mail-sending step |
| `ABORT` | `.error(new AbortException(...))` |
| `SHELL` | `ProcessBuilder` wrapped in `Mono.fromCallable()` |
| `JOB` (sub-job) | `jobOrchestrator.execute(subJobDef, ctx)` |
| `TRANS` (sub-trans) | `transformPipeline.execute(subTransDef, ctx)` |

### 6.4 Handling the Long Tail

With 1,500 jobs, there will be a "long tail" of rare step types. Strategy:

1. **Audit first**: Parse all 1,500 files, count step types by frequency
2. **80/20 rule**: The top ~15 step types will cover ~80%+ of all steps
3. **Build generators for the top types first**
4. **Scripting escape hatch**: For rare/complex steps, generate a `ScriptStep` that embeds the original logic
5. **Manual review queue**: Flag jobs that use unsupported types for human review

---

## 7. Migration Phases

### Phase 1: Discovery & Analysis (Weeks 1-3)

- [ ] Build Pentaho XML parser for .kjb and .ktr files
- [ ] Parse all 1,500 jobs/transformations into IR
- [ ] Generate a **migration inventory report**:
  - Step type frequency distribution
  - Job complexity (step count, hop count, nesting depth)
  - Connection types used (DB, file, REST, etc.)
  - Parameter/variable usage patterns
  - Identify clusters of similar jobs
- [ ] Classify jobs by migration difficulty: Auto / Assisted / Manual
- [ ] Prioritize which step types to support in code generator

### Phase 2: Engine Foundation (Weeks 3-7)

- [ ] Set up Spring Boot WebFlux project structure
- [ ] Implement core abstractions: `DataRow`, `StepExecutor`, `RowTransformer`
- [ ] Implement `JobOrchestrator` (graph walker → Mono chain builder)
- [ ] Implement `TransformationPipeline` (Flux chain builder)
- [ ] Implement `JobContext` with reactive context propagation
- [ ] Build connector layer: R2DBC, JDBC-elastic, WebClient, File I/O
- [ ] Build observability: metrics, tracing, structured logging
- [ ] Build the top 10-15 step executors (DB input/output, file I/O, select, filter, etc.)
- [ ] Integration test framework with embedded DB and mock servers

### Phase 3: Code Generator (Weeks 6-10)

- [ ] Build code generator: IR → Java source files
- [ ] Template per step type (start with top 15)
- [ ] Handle hop wiring (OK/Error/Unconditional → flatMap/onErrorResume/then)
- [ ] Handle parallel branches (fan-out/fan-in)
- [ ] Handle sub-job and sub-transformation references
- [ ] Handle parameter/variable substitution
- [ ] Output: compilable Spring `@Component` classes per job
- [ ] Validation: compile check + static analysis on generated code

### Phase 4: Batch Migration (Weeks 9-16)

- [ ] Run code generator against all 1,500 jobs
- [ ] Triage output:
  - **Green**: Fully auto-generated, compiles, ready for parity testing
  - **Yellow**: Generated with warnings, needs human review
  - **Red**: Failed generation, requires manual migration
- [ ] Iterate on generator to move Yellow → Green
- [ ] Manual migration for Red jobs (expect ~5-10% of total)
- [ ] Domain-team reviews of generated code

### Phase 5: Parity Testing (Weeks 14-20)

- [ ] Build parity test framework:
  - Run Pentaho job with known input → capture output
  - Run Java job with same input → capture output
  - Diff outputs row-by-row
- [ ] Automate parity tests for Green jobs
- [ ] Fix discrepancies (type coercion, null handling, ordering, etc.)
- [ ] Performance benchmarks: Java vs Pentaho throughput

### Phase 6: Cutover (Weeks 18-24)

- [ ] Shadow mode: run both Pentaho and Java in parallel, compare results
- [ ] Gradual cutover: migrate by domain/priority
- [ ] Monitoring dashboards for all migrated jobs
- [ ] Decommission Pentaho jobs domain by domain
- [ ] Knowledge transfer and documentation

---

## 8. Key Design Decisions to Make

| Decision | Options | Recommendation |
|---|---|---|
| **Blocking vs Reactive DB** | R2DBC everywhere vs JDBC on elastic scheduler | Start with JDBC-on-elastic (broader driver support), migrate hot paths to R2DBC |
| **Row representation** | Generic Map vs typed POJOs | Generic `DataRow` (Map-backed) for flexibility; typed POJOs for high-frequency paths |
| **Job definition format** | Pure Java code vs external DSL/YAML | Java code (type-safe, IDE support, testable, debuggable) |
| **Scheduling** | Spring `@Scheduled` vs Quartz vs external (Airflow/K8s CronJob) | External scheduler triggering via REST API (decouples scheduling from execution) |
| **Error handling** | Fail-fast vs configurable retry | Configurable per-step: retry count, backoff, dead-letter |
| **State management** | Stateless vs checkpoint/resume | Stateless first; add checkpointing for long-running jobs later |
| **Script steps** | GraalVM polyglot vs Groovy | Groovy (mature Spring integration, familiar to Java devs) |
| **Testing strategy** | Unit per step vs E2E parity | Both: unit tests for steps, parity tests for full job equivalence |

---

## 9. Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| Long tail of rare Pentaho step types | Blocks full automation | Scripting escape hatch + manual migration budget |
| Pentaho implicit behaviors (type coercion, null handling) | Parity failures | Extensive parity testing; document known differences |
| Reactive learning curve for team | Slow development | Training sessions; pair programming; keep blocking wrappers as escape hatch |
| **OOM on Sort step with 2GB+ CSVs** | Job crashes, data loss | External merge sort with disk spill; all other ops (GroupBy, Dedupe, Join) stream on sorted input (see Section 10) |
| Performance regression | SLA violations | Benchmark early; keep JDBC option for problematic queries |
| Scope creep (improving jobs during migration) | Timeline slippage | Strict "lift and shift" first, optimize later |
| Connection/credential management differences | Runtime failures | Externalize all configs; use Spring profiles + vault |

---

## 10. Large File & Blocking Step Strategy (CSV 2GB+)

### The Problem

With CSV files exceeding 2GB (tens of millions of rows), a naive `collectSortedList()`
on 50M rows will blow the heap. However, **Sort is the ONLY truly blocking operation**.
All other "blocking" Pentaho steps (Group By, Unique Rows, Merge Join) can be
implemented as **streaming operators** — they just need sorted input.

### Classification: Truly Blocking vs Streaming

| Pentaho Step | Truly Blocking? | Why / How to Stream |
|---|---|---|
| **Sort Rows** | **YES** — the only one | Must see all rows before emitting; needs external merge sort |
| **Group By** | **NO** — streaming | On sorted input: consecutive keys → `bufferUntilChanged()`, O(1) memory. On unsorted with low cardinality: HashMap accumulator, still O(1) relative to row count |
| **Unique Rows** | **NO** — streaming | On sorted input: compare to previous row, O(1) memory |
| **Merge Join** | **NO** — streaming | Two sorted streams: advance pointers, O(1) memory |
| **Append Streams** | **NO** — streaming | `Flux.concat()` — inherently streaming |
| **Analytic Query** | **NO** — streaming | Partition + sort → sliding window over sorted partitions |

### Strategy: External Sort (Disk-Backed)

For datasets that exceed a configurable memory threshold, use **external merge sort**:

```
              CSV (2GB+)
                 │
                 ▼
    ┌──────────────────────┐
    │  Chunked Reader       │  Read N rows at a time (e.g., 100K rows)
    │  Flux<DataRow>        │  Backpressure-aware, never loads full file
    └──────────┬───────────┘
               │
               ▼
    ┌──────────────────────┐
    │  In-Memory Sort       │  Sort each chunk in memory
    │  per chunk            │  (100K rows fits easily in heap)
    └──────────┬───────────┘
               │
               ▼
    ┌──────────────────────┐
    │  Spill to Temp Files  │  Write each sorted chunk to disk
    │  (sorted runs)        │  /tmp/sort-run-001.csv, 002, 003...
    └──────────┬───────────┘
               │
               ▼
    ┌──────────────────────┐
    │  K-Way Merge          │  Merge sorted runs using min-heap
    │  Flux<DataRow>        │  Emits globally sorted rows as a Flux
    │  (streaming output)   │  Memory = O(K) where K = number of runs
    └──────────────────────┘
```

### Design: `ExternalSortOperator`

```java
public class ExternalSortOperator implements RowTransformer {

    private final List<SortField> sortFields;
    private final int chunkSize;           // rows per chunk (default: 100_000)
    private final long memoryThreshold;    // bytes before spilling to disk
    private final Path spillDir;           // temp directory for sorted runs

    @Override
    public Flux<DataRow> apply(Flux<DataRow> input, TransformContext context) {
        return input
            .buffer(chunkSize)                           // collect chunks
            .flatMapSequential(chunk -> sortAndSpill(chunk))  // sort + write to disk
            .collectList()                               // collect run file paths
            .flatMapMany(runs -> kWayMerge(runs));       // merge sorted runs → Flux
    }

    /** Sort chunk in-memory, write to temp file, return path */
    private Mono<Path> sortAndSpill(List<DataRow> chunk) {
        return Mono.fromCallable(() -> {
            chunk.sort(buildComparator());
            Path runFile = spillDir.resolve("run-" + counter.getAndIncrement() + ".dat");
            writeToDisk(runFile, chunk);
            return runFile;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /** K-way merge of sorted run files using a priority queue */
    private Flux<DataRow> kWayMerge(List<Path> runFiles) {
        return Flux.create(sink -> {
            // Open a buffered reader per run file
            // Priority queue of (currentRow, readerIndex) sorted by sort fields
            // Poll min, emit, advance that reader, repeat
            // Memory footprint: O(K) where K = number of run files
        });
    }
}
```

### Design: Streaming CSV Reader

The CSV reader itself must be non-buffering — never load the full file:

```java
public class ReactiveCsvReader {

    /**
     * Reads a CSV file as a Flux<DataRow>, streaming line by line.
     * Supports files of any size — memory usage is O(1) per row.
     * Applies backpressure: only reads as fast as downstream consumes.
     */
    public Flux<DataRow> read(Path csvFile, CsvConfig config) {
        return Flux.using(
            () -> Files.newBufferedReader(csvFile, config.charset()),
            reader -> Flux.fromStream(reader.lines())
                          .skip(config.hasHeader() ? 1 : 0)
                          .map(line -> parseLine(line, config)),
            reader -> closeQuietly(reader)
        ).subscribeOn(Schedulers.boundedElastic());
    }
}
```

### Memory Budget & Thresholds

| Parameter | Default | Notes |
|---|---|---|
| `sort.chunk-size` | 100,000 rows | Tunable per job; ~50-100MB per chunk |
| `sort.memory-threshold` | 512MB | If dataset estimate exceeds this, force external sort |
| `sort.spill-dir` | `/tmp/etl-sort/` | Must have sufficient disk space |
| `sort.auto-detect` | `true` | Estimate dataset size from file size; auto-switch strategy |
| `csv.buffer-size` | 8KB | BufferedReader buffer |

### Decision Logic: In-Memory vs External Sort

```java
public RowTransformer createSortStep(SortConfig config, long estimatedRows) {
    if (estimatedRows < config.getInMemoryThreshold()) {
        // Small dataset: simple in-memory sort (fast, no disk I/O)
        return input -> input.collectSortedList(comparator).flatMapMany(Flux::fromIterable);
    } else {
        // Large dataset: external merge sort (bounded memory, uses disk)
        return new ExternalSortOperator(config);
    }
}
```

### Streaming Operators (NOT Blocking)

These all operate on sorted input and use O(1) memory relative to total row count:

#### Streaming Group By

```java
public class StreamingGroupByOperator implements RowTransformer {

    @Override
    public Flux<DataRow> apply(Flux<DataRow> sortedInput, TransformContext ctx) {
        // Input MUST be sorted by group key (engine ensures this)
        return sortedInput
            .bufferUntilChanged(row -> row.get(groupKey))  // consecutive groups
            .map(group -> aggregate(group));                // emit one row per group
        // Memory: only holds ONE group at a time
    }

    // For unsorted input with LOW cardinality (< threshold):
    public Flux<DataRow> applyUnsorted(Flux<DataRow> input, TransformContext ctx) {
        Map<Object, Accumulator> accumulators = new ConcurrentHashMap<>();
        return input
            .doOnNext(row -> accumulators
                .computeIfAbsent(row.get(groupKey), k -> new Accumulator())
                .add(row))
            .then(Mono.fromCallable(() -> accumulators.values()))
            .flatMapMany(Flux::fromIterable)
            .map(Accumulator::toRow);
        // Memory: O(G) where G = number of distinct groups (must be small)
    }
}
```

#### Streaming Dedupe (Unique Rows)

```java
public class StreamingDedupeOperator implements RowTransformer {

    @Override
    public Flux<DataRow> apply(Flux<DataRow> sortedInput, TransformContext ctx) {
        return sortedInput.distinctUntilChanged(row -> row.get(dedupeKey));
        // Memory: O(1) — only holds previous row for comparison
    }
}
```

#### Streaming Merge Join

```java
public class StreamingMergeJoinOperator {

    public Flux<DataRow> join(Flux<DataRow> left, Flux<DataRow> right, String joinKey) {
        // Both inputs pre-sorted on joinKey
        // Advance two pointers, emit matches — classic merge join
        // Memory: O(1) per pair
    }
}
```

### Key Principles

> 1. **Sort is the only truly blocking operation.** Everything else streams.
> 2. **External merge sort** is used only for Sort — bounded memory via disk spill.
> 3. **All other operators chain on sorted output** — O(1) memory per row.
> 4. **The engine auto-detects**: if a Group By / Dedupe / Join sees unsorted input,
>    it inserts an external sort on the required key before the streaming operator.

---

## 11. Success Metrics

- **Automation rate**: % of jobs fully auto-generated (target: >85%)
- **Parity pass rate**: % of jobs producing identical output to Pentaho (target: >99%)
- **Performance**: P95 latency and throughput equal or better than Pentaho
- **Operational**: All jobs observable via metrics/traces/logs
- **Timeline**: Full cutover within 24 weeks

---

## 11. Technology Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21+, Spring Boot 3.x, Spring WebFlux |
| Reactive | Project Reactor (Mono/Flux) |
| Reactive DB | R2DBC (Postgres, MySQL, Oracle) |
| Blocking DB | HikariCP + Schedulers.boundedElastic() |
| HTTP | Spring WebClient |
| Messaging | Reactor Kafka / Spring Cloud Stream |
| File I/O | Reactive Streams file reading, Apache POI (Excel) |
| Scripting | Groovy (via GroovyShell) |
| Observability | Micrometer + Prometheus, OpenTelemetry, Logback structured |
| Testing | JUnit 5, reactor-test StepVerifier, Testcontainers |
| Build | Maven multi-module |
| Code Gen | JavaPoet or Roaster for Java source generation |
| Pentaho Parsing | JAXB or Jackson XML for .kjb/.ktr parsing |
