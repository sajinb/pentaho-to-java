# Pentaho-to-Java Migration Plan

## 1. Problem Statement

Migrate **1,500 Pentaho jobs/transformations** (.kjb/.ktr) to a Java-based engine with a clean architectural split:

- **Orchestration (.kjb)** → **Spring Reactor (Mono)** — async step sequencing, parallel fan-out/fan-in, error routing
- **Transformation (.ktr)** → **Plain Java (Stream/Iterator)** — row-by-row data processing, CPU-bound, simple and debuggable

### Why Migrate?

- Pentaho's visual ETL designer doesn't scale well for version control, testing, and CI/CD
- XML-based job/transformation definitions are fragile and hard to refactor
- Limited observability, retry semantics, and backpressure handling
- Licensing and operational costs
- Reactor for orchestration gives non-blocking async coordination
- Plain Java for transformations keeps code simple, debuggable, and maintainable

---

## 2. Pentaho Concepts → Java Mapping

### Orchestration (.kjb) → Reactor Mono

| Pentaho Concept | Java Equivalent |
|---|---|
| **Job (.kjb)** | `Mono<JobResult>` orchestration chain |
| **Job Entry (step)** | `StepExecutor` returning `Mono<StepResult>` |
| **Hop (OK/Error/Unconditional)** | `.flatMap()` / `.onErrorResume()` / `.then()` |
| **Variables / Parameters** | `JobContext` (immutable map, passed through chain) |
| **Sub-job** | Nested `Mono<StepResult>` composition |
| **Parallel execution** | `Mono.zip(step1, step2, step3)` |
| **Sequential execution** | `Mono.then()` / `.flatMap()` chaining |
| **Conditional hop** | `Mono.defer(() -> condition ? stepA : stepB)` |
| **Error handling** | `.onErrorResume()` / `.retry()` / `.onErrorMap()` |
| **REST calls** | `WebClient` returning `Mono<T>` |

### Transformation (.ktr) → Plain Java

| Pentaho Concept | Java Equivalent |
|---|---|
| **Transformation (.ktr)** | `TransformationPipeline` using `Iterator<DataRow>` / `Stream<DataRow>` |
| **Transformation Step** | `RowTransformer` — plain Java function |
| **Row** | `DataRow` (Map-backed or POJO) |
| **File I/O** | `BufferedReader` / `BufferedWriter` (standard Java I/O) |
| **Database** | JDBC `PreparedStatement` (standard, blocking — it's fine) |
| **Select Values** | `.map(row -> row.select("col1","col2"))` |
| **Filter Rows** | `.filter(row -> condition)` |
| **Sort Rows** | `IndexSortOperator` (Record-based, see Section 10) |
| **Group By** | `StreamingGroupBy` on sorted input |
| **Logging** | SLF4J + MDC (standard, no reactive context needed) |
| **Error handling** | `try/catch` — simple, debuggable stack traces |

---

## 3. High-Level Architecture

```
1,500 Job Definitions (auto-generated) feed into the DAG Executor.
Each is pure data: which steps to use + how they're wired (hops).

  JobDefinitions --> 1 Common DAG Executor (Reactor Mono)
                        --> 85 Step Components (@FunctionalInterface -> Mono<StepResult>)
                            --> Transformation Engine (Plain Java Iterator)
                                --> Connector Layer (JDBC / File I/O / WebClient / SFTP / S3)
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
├── dag-executor/                        # Common DAG engine — REACTOR (Mono)
│   ├── core/                            # Core abstractions
│   │   ├── model/                       # JobDefinition, JobGraph, HopType
│   │   ├── engine/                      # DagExecutor (walks graph, chains Monos)
│   │   ├── context/                     # JobContext, variable resolution
│   │   └── error/                       # Error handling, retry, timeout policies
│   └── observability/                   # Monitoring & tracing (orchestration)
│
├── step-components/                     # 85 reusable step components (@FunctionalInterface)
│   ├── spi/                             # StepExecutor interface + StepResult
│   ├── db/                              # SqlQueryStep, SqlExecuteStep, StoredProcStep (~12)
│   ├── file/                            # FileCopy, FileDelete, FileExists, FileMove (~8)
│   ├── file-transfer/                   # SftpGet, SftpPut, FtpGet, S3Upload (~8)
│   ├── rest/                            # HttpGetStep, HttpPostStep, RestCallStep (~5)
│   ├── messaging/                       # KafkaProduceStep, KafkaConsumeStep, JmsSend (~5)
│   ├── shell/                           # ShellCommandStep, SshExecStep (~3)
│   ├── mail/                            # MailStep, SlackStep (~4)
│   ├── flow/                            # StartStep, SuccessStep, AbortStep, DummyStep (~8)
│   ├── orchestration/                   # RunJobStep (sub-job), WaitForFile, Delay (~5)
│   ├── transform/                       # RunTransformationStep (bridge to plain Java) (~1)
│   ├── validation/                      # CheckDbConnection, TableExists, FileExists (~5)
│   ├── variable/                        # SetVariable, EvalCondition, WriteToLog (~5)
│   └── scripting/                       # GroovyScriptStep (escape hatch) (~1)
│                                        # Total: ~85 step components
│
├── transformation-engine/               # Data transformation — PLAIN JAVA
│   ├── core/                            # DataRow, TransformationPipeline (Iterator-based)
│   ├── steps/                           # CsvReader, TableInput, SelectValues, Sort, GroupBy, Filter
│   ├── connectors/                      # JDBC + HikariCP, SFTP, S3
│   └── observability/                   # Micrometer, OpenTelemetry, SLF4J + MDC
│
├── generated-jobs/                      # Auto-generated Java jobs (output of migration)
│
├── admin-api/                           # Spring WebFlux REST API (optional)
│
└── tests/
    ├── integration/                     # End-to-end pipeline tests
    ├── parity/                          # Output comparison: Pentaho vs Java
    └── performance/                     # Throughput & latency benchmarks
```

---

## 5. Core Abstractions (Design)

### 5.1 StepExecutor — The Functional Interface (85 components implement this)

```java
@FunctionalInterface
public interface StepExecutor {
    Mono<StepResult> execute(JobContext context);
}

public record StepResult(
    StepStatus status,       // SUCCESS, FAILURE, SKIPPED
    JobContext context,       // Potentially enriched context
    Map<String, Object> outputs
) {
    public static StepResult success(JobContext ctx) {
        return new StepResult(StepStatus.SUCCESS, ctx, Map.of());
    }
    public static StepResult failure(JobContext ctx, String reason) {
        return new StepResult(StepStatus.FAILURE, ctx, Map.of("error", reason));
    }
}
```

### 5.2 JobDefinition + JobGraph — Pure Data (1,500 auto-generated)

```java
public interface JobDefinition {
    JobGraph define(JobContext ctx);
}

public class JobGraph {
    private final Map<String, StepExecutor> steps;
    private final List<Hop> hops;

    public record Hop(String from, String to, HopType type) {}
    public enum HopType { OK, ERROR, UNCONDITIONAL }

    public List<List<String>> topologicalLevels() { ... }
    public List<String> getHops(String stepId, HopType type) { ... }
    public static Builder builder() { return new Builder(); }
}
```

### 5.3 DagExecutor — The Common Engine (ONE, shared by all 1,500 jobs)

```java
public class DagExecutor {

    public Mono<JobResult> execute(JobDefinition job, JobContext ctx) {
        JobGraph graph = job.define(ctx);
        return executeFromStep(graph, graph.startStep(), ctx)
                .map(r -> new JobResult(r.status()));
    }

    private Mono<StepResult> executeFromStep(JobGraph graph, String stepId, JobContext ctx) {
        StepExecutor step = graph.getStep(stepId);

        return step.execute(ctx)
            .timeout(graph.getTimeout(stepId))
            .retry(graph.getRetryCount(stepId))
            .flatMap(result -> {
                if (result.status() == SUCCESS) {
                    return followHops(graph, stepId, HopType.OK, result.context());
                } else {
                    return followHops(graph, stepId, HopType.ERROR, result.context());
                }
            })
            .onErrorResume(e ->
                followHops(graph, stepId, HopType.ERROR, ctx.with("error", e.getMessage()))
            );
    }

    private Mono<StepResult> followHops(JobGraph graph, String stepId,
                                         HopType type, JobContext ctx) {
        List<String> nextSteps = graph.getHops(stepId, type);

        if (nextSteps.isEmpty()) {
            return Mono.just(StepResult.success(ctx));
        }
        if (nextSteps.size() == 1) {
            return executeFromStep(graph, nextSteps.get(0), ctx);
        }

        // Parallel fan-out
        List<Mono<StepResult>> parallel = nextSteps.stream()
            .map(next -> executeFromStep(graph, next, ctx))
            .toList();
        return Mono.zip(parallel, results -> mergeResults(results, ctx));
    }
}
```

### 5.4 Example Step Components

```java
// Database
@Component
public class SqlQueryStep implements StepExecutor {
    @Override
    public Mono<StepResult> execute(JobContext ctx) {
        return Mono.fromCallable(() -> {
            try (var conn = dataSource.getConnection();
                 var stmt = conn.prepareStatement(ctx.param("query"))) {
                stmt.execute();
                return StepResult.success(ctx.with("rowCount", stmt.getUpdateCount()));
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
}

// REST Call (natively non-blocking)
@Component
public class HttpPostStep implements StepExecutor {
    @Override
    public Mono<StepResult> execute(JobContext ctx) {
        return webClient.post()
            .uri(ctx.param("url"))
            .bodyValue(ctx.param("payload"))
            .retrieve()
            .bodyToMono(String.class)
            .map(resp -> StepResult.success(ctx.with("response", resp)));
    }
}

// Run Transformation (bridge to plain Java)
@Component
public class RunTransformationStep implements StepExecutor {
    @Override
    public Mono<StepResult> execute(JobContext ctx) {
        return Mono.fromCallable(() -> {
            pipeline.execute(ctx.param("transformDef"), ctx.toTransformContext());
            return StepResult.success(ctx);
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
```

### 5.5 Auto-Generated JobDefinition Example

```java
@Component
public class CustomerDailyLoadJob implements JobDefinition {

    @Autowired SqlQueryStep sqlQuery;
    @Autowired RunTransformationStep runTransform;
    @Autowired MailStep mail;

    @Override
    public JobGraph define(JobContext ctx) {
        return JobGraph.builder()
            .step("start",     Steps.start())
            .step("extract",   sqlQuery.with(ctx.param("source_query")))
            .step("transform", runTransform.with("customer_transform_v2"))
            .step("load",      sqlQuery.with(ctx.param("insert_query")))
            .step("notify",    mail.with(ctx.param("success_email")))
            .step("onError",   mail.with(ctx.param("error_email")))

            .hop("start",     "extract",   UNCONDITIONAL)
            .hop("extract",   "transform", OK)
            .hop("transform", "load",      OK)
            .hop("load",      "notify",    OK)
            .hop("extract",   "onError",   ERROR)
            .hop("transform", "onError",   ERROR)
            .hop("load",      "onError",   ERROR)
            .build();
    }
}
```

### 5.6 Transformation Engine — Plain Java

```java
public interface DataRow {
    Object get(String field);
    long getByteOffset();
    DataRow with(String field, Object value);
    DataRow without(String field);
    Map<String, Object> toMap();
    Set<String> fieldNames();
}

public interface RowTransformer {
    Iterator<DataRow> apply(Iterator<DataRow> input, TransformContext context);
}

public class TransformationPipeline {
    public TransformResult execute(TransformationDefinition def, TransformContext ctx) {
        Iterator<DataRow> pipeline = sourceStep.read(ctx);
        for (RowTransformer step : transformSteps) {
            pipeline = step.apply(pipeline, ctx);
        }
        sinkStep.write(pipeline, ctx);
        return new TransformResult(ctx.getMetrics());
    }
}
```

---

## 6. Automation Engine: Pentaho XML → JobDefinition Classes

The code generator produces pure data classes (JobGraph declarations) that
reference the 85 step components. No execution logic is generated.

### 6.1 Pipeline

```
.kjb files (1,500) → XML Parser → IR (JobIR/StepIR/HopIR) → Code Generator → JobDefinition.java
```

### 6.2 Intermediate Representation (IR)

```java
public record JobIR(String name, Map<String, String> parameters,
                    List<StepIR> steps, List<HopIR> hops) {}
public record StepIR(String id, String type, Map<String, Object> config) {}
public record HopIR(String from, String to, HopType type) {}
```

### 6.3 Step Type Mapping

| Pentaho Step Type | Generated Code |
|---|---|
| `START` | `Steps.start()` |
| `SQL` | `sqlExecuteStep.with(query)` |
| `HTTP` | `httpPostStep.with(url, payload)` |
| `MAIL` | `mailStep.with(to, subject, body)` |
| `SHELL` | `shellCommandStep.with(command)` |
| `JOB` (sub-job) | `runJobStep.with(subJobDef)` |
| `TRANS` (sub-trans) | `runTransformationStep.with(transformDef)` |

### 6.4 Build Priority (85 Step Components)

| Priority | Count | Coverage |
|---|---|---|
| **P1** — SqlQuery, RunTransformation, RunJob, Start, Success, Mail, Shell, etc. | ~15 | ~80% of jobs |
| **P2** — StoredProc, TableExists, KafkaProduce, S3Upload, SshExec, etc. | ~35 | ~95% of jobs |
| **P3** — Remaining rare steps | ~35 | 100% |
| **Escape hatch** — GroovyScriptStep | 1 | Fallback |

---

## 7. Migration Phases

### Phase 1: Discovery & Analysis (Weeks 1-3)
- Build XML parser for .kjb and .ktr files
- Parse all 1,500 jobs into IR
- Generate migration inventory report
- Classify jobs by difficulty: Auto / Assisted / Manual

### Phase 2: Engine Foundation (Weeks 3-7)
- Implement StepExecutor, JobDefinition, JobGraph, DagExecutor
- Implement TransformationPipeline (plain Java Iterator chain)
- Build P1 step components (~15)
- Integration test framework

### Phase 3: Code Generator (Weeks 6-10)
- Build code generator: IR → JobDefinition Java source files
- Handle sub-job and sub-transformation references
- Build P2 step components (~35)

### Phase 4: Batch Migration (Weeks 9-16)
- Run code generator against all 1,500 jobs
- Triage: Green (auto) / Yellow (review) / Red (manual)
- Iterate to move Yellow → Green

### Phase 5: Parity Testing (Weeks 14-20)
- Run Pentaho vs Java side-by-side, diff outputs
- Fix discrepancies
- Performance benchmarks

### Phase 6: Cutover (Weeks 18-24)
- Shadow mode → gradual domain-by-domain cutover
- Decommission Pentaho

---

## 8. Key Design Decisions

| Decision | Choice |
|---|---|
| **Orchestration** | Reactor Mono for .kjb orchestration |
| **Transformation** | Plain Java Iterator for .ktr transforms |
| **DB (transforms)** | JDBC + HikariCP |
| **DB (orchestration)** | JDBC on boundedElastic scheduler |
| **Row representation** | Generic DataRow (Map-backed) |
| **Job definition format** | Java code (type-safe, IDE support) |
| **Scheduling** | External scheduler via REST API |
| **Error handling** | Configurable per-step: retry, backoff, dead-letter |
| **Script steps** | Groovy (escape hatch) |
| **Testing (transforms)** | Plain JUnit assertions |
| **Testing (orchestration)** | StepVerifier for Mono chains |

---

## 9. Risk Register

| Risk | Mitigation |
|---|---|
| Long tail of rare step types | Groovy escape hatch + manual budget |
| Pentaho implicit behaviors | Extensive parity testing |
| Reactive learning curve | Training; blocking wrappers as escape hatch |
| OOM on Sort with 2GB+ CSVs | Record index sort (see Section 10) |
| Performance regression | Benchmark early; keep JDBC option |
| Scope creep | Strict lift-and-shift first |

---

## 10. Large File & Sort Strategy (CSV 2GB+)

### Sort is the ONLY truly blocking operation

Group By, Unique Rows, Merge Join are all streaming on sorted input.

### Three-Tier Sort Strategy

| Tier | When | How | Memory |
|---|---|---|---|
| **1. Full in-memory** | < 500K rows | `collectSortedList()` | Full dataset |
| **2. Record index sort** | 500K-25M rows | `Record(sortKeys, byteOffset)` ~40 bytes/entry | ~160MB for 4M rows |
| **3. External disk sort** | 25M+ rows | Chunk → spill → k-way merge | Disk-backed |

### Record Index Sort (Primary Strategy)

```java
public record SortEntry(
    Object[] sortKeys,
    long byteOffset
) implements Comparable<SortEntry> {}

public class IndexSortOperator implements RowTransformer {
    @Override
    public Iterator<DataRow> apply(Iterator<DataRow> input, TransformContext ctx) {
        // Pass 1: Build lightweight sorted index
        List<SortEntry> entries = new ArrayList<>();
        while (input.hasNext()) {
            DataRow row = input.next();
            entries.add(new SortEntry(extractSortKeys(row), row.getByteOffset()));
        }
        SortEntry[] array = entries.toArray(SortEntry[]::new);
        Arrays.parallelSort(array);

        // Pass 2: Read full rows in sorted order via RandomAccessFile
        return new SortedRowIterator(array, sourceFile);
    }
}
```

### Streaming Operators (Plain Java, O(1) memory)

- **Group By**: `bufferUntilChanged` on sorted input, holds 1 group at a time
- **Dedupe**: `distinctUntilChanged`, holds previous row only
- **Merge Join**: Two sorted iterators, advance pointers

### Key Principles

1. Reactor for orchestration (.kjb), plain Java for transformation (.ktr)
2. Sort is the only truly blocking operation — everything else streams
3. Record index sort is primary strategy — external sort is fallback only
4. All other operators chain on sorted Iterator output — O(1) memory
5. RunTransformationStep bridges plain Java into Mono via Mono.fromCallable()
6. Transformation stack traces are normal Java — simple debugging

---

## 11. Success Metrics

- **Automation rate**: >85% of jobs fully auto-generated
- **Parity pass rate**: >99% identical output to Pentaho
- **Performance**: P95 equal or better than Pentaho
- **Operational**: All jobs observable via metrics/traces/logs
- **Timeline**: Full cutover within 24 weeks

---

## 12. Technology Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21+, Spring Boot 3.x |
| Orchestration (.kjb) | Spring WebFlux + Project Reactor (Mono) |
| Transformation (.ktr) | Plain Java (Iterator/Stream) |
| Database (transforms) | JDBC + HikariCP |
| Database (orchestration) | JDBC via Schedulers.boundedElastic() |
| HTTP | Spring WebClient |
| File I/O | BufferedReader / RandomAccessFile / MappedByteBuffer |
| Excel | Apache POI (streaming SXSSF) |
| Scripting | Groovy (via GroovyShell) |
| Observability | Micrometer + Prometheus, OpenTelemetry, SLF4J + Logback |
| Testing | JUnit 5, StepVerifier, Testcontainers |
| Build | Maven multi-module |
| Code Gen | JavaPoet or Roaster |
| Pentaho Parsing | JAXB or Jackson XML |
