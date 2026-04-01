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
┌─────────────────────────────────────────────────────────────────────┐
│                    API / Trigger Layer (WebFlux)                      │
│  REST endpoints, Scheduler/Cron, Kafka consumers, File watch         │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ triggers
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│              1 Common DAG Executor  ← REACTOR (Mono)                 │
│                                                                      │
│  Takes any JobDefinition (graph of steps + hops)                     │
│  Chains steps via .flatMap() / Mono.zip() / .onErrorResume()         │
│  Handles: sequencing, parallelism, conditionals, error routing       │
│  Retry, timeout, logging — all built into the common engine          │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ invokes
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│         85 Step Components  ← @FunctionalInterface → Mono<StepResult>│
│                                                                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │
│  │ SqlQuery │ │ RestCall │ │ SftpGet  │ │ MailSend │ │ RunTrans │  │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └─────┬────┘  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │       │
│  │ ShellCmd │ │ KafkaPub │ │ FileCopy │ │ RunJob   │       │       │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │       │
│  ... (85 total, all reusable across 1,500 jobs)             │       │
└─────────────────────────────────────────────────────────────┼───────┘
                                                              │ bridge
                                                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│             Transformation Engine  ← PLAIN JAVA (Iterator)           │
│                                                                      │
│  Iterator<DataRow> pipeline: read → map → filter → sort → write      │
│  Simple, debuggable, no reactive types                               │
└─────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Connector Layer                                │
│  JDBC / BufferedReader / BufferedWriter / WebClient / SFTP / S3      │
└─────────────────────────────────────────────────────────────────────┘

1,500 Job Definitions (auto-generated) feed into the DAG Executor.
Each is pure data: which steps to use + how they're wired (hops).
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
│   │
│   └── observability/                   # Monitoring & tracing (orchestration)
│       ├── metrics/                     # Micrometer (step duration, success/fail)
│       ├── tracing/                     # OpenTelemetry spans per step
│       └── logging/                     # Structured logging per step execution
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
│   │                                    # Total: ~85 step components
│   │
│   └── connectors/                      # Shared connection management
│       ├── webclient/                   # Spring WebClient (REST)
│       ├── jdbc/                        # JDBC DataSource pool
│       ├── sftp/                        # JSch / Apache SSHD
│       ├── kafka/                       # Reactor Kafka
│       └── s3/                          # AWS SDK
│
├── transformation-engine/               # Data transformation — PLAIN JAVA
│   ├── core/                            # Core abstractions
│   │   ├── model/                       # DataRow, TransformationDefinition
│   │   ├── pipeline/                    # TransformationPipeline (Iterator-based)
│   │   └── context/                     # TransformContext, variable resolution
│   │
│   ├── steps/                           # Built-in transform steps (plain Java)
│   │   ├── io/                          # CsvReader, CsvWriter, ExcelReader, FileOutput
│   │   ├── db/                          # TableInput (JDBC), TableOutput, DBLookup
│   │   ├── transform/                   # SelectValues, Calculator, ValueMapper
│   │   ├── sort/                        # IndexSortOperator, InMemorySort
│   │   ├── aggregate/                   # StreamingGroupBy, StreamingDedupe
│   │   ├── join/                        # StreamingMergeJoin, HashLookupJoin
│   │   ├── flow/                        # Filter, SwitchCase, Abort
│   │   └── scripting/                   # Groovy script step (escape hatch)
│   │
│   ├── connectors/                      # Blocking I/O (plain Java — no wrappers needed)
│   │   ├── jdbc/                        # JDBC + HikariCP
│   │   ├── sftp/                        # JSch / Apache SSHD
│   │   └── s3/                          # AWS SDK
│   │
│   └── observability/                   # Monitoring & tracing
│       ├── metrics/                     # Micrometer (rows processed, throughput)
│       ├── tracing/                     # OpenTelemetry spans per step
│       └── logging/                     # SLF4J + MDC (standard, simple)
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

### 5.1 StepExecutor — The Functional Interface (85 components implement this)

```java
/**
 * Every one of the 85 step components implements this single interface.
 * It is the only contract between the DAG executor and the step components.
 */
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
/**
 * Each .kjb becomes one JobDefinition. It declares WHAT steps to run and
 * HOW they're wired — no execution logic. The DAG executor handles the rest.
 */
public interface JobDefinition {
    JobGraph define(JobContext ctx);
}

public class JobGraph {
    private final Map<String, StepExecutor> steps;   // step ID → executor
    private final List<Hop> hops;                     // directed edges

    public record Hop(String from, String to, HopType type) {}
    public enum HopType { OK, ERROR, UNCONDITIONAL }

    /** Returns steps grouped by topological level (for parallelism detection) */
    public List<List<String>> topologicalLevels() { ... }

    /** Returns hop targets for a given step and hop type */
    public List<String> getHops(String stepId, HopType type) { ... }

    public static Builder builder() { return new Builder(); }
}
```

### 5.3 DagExecutor — The Common Engine (ONE, shared by all 1,500 jobs)

```java
/**
 * Common DAG executor. Walks ANY JobGraph and chains steps using Mono.
 * Never changes per job — all 1,500 jobs run through this one engine.
 */
public class DagExecutor {

    public Mono<JobResult> execute(JobDefinition job, JobContext ctx) {
        JobGraph graph = job.define(ctx);
        return executeFromStep(graph, graph.startStep(), ctx)
                .map(r -> new JobResult(r.status()));
    }

    private Mono<StepResult> executeFromStep(JobGraph graph, String stepId, JobContext ctx) {
        StepExecutor step = graph.getStep(stepId);

        return step.execute(ctx)                                 // run this step
            .doOnSubscribe(s -> log.info("Starting step: {}", stepId))
            .doOnSuccess(r -> log.info("Step {} → {}", stepId, r.status()))
            .timeout(graph.getTimeout(stepId))                   // per-step timeout
            .retry(graph.getRetryCount(stepId))                  // per-step retry
            .flatMap(result -> {
                if (result.status() == SUCCESS) {
                    return followHops(graph, stepId, HopType.OK, result.context());
                } else {
                    return followHops(graph, stepId, HopType.ERROR, result.context());
                }
            })
            .onErrorResume(e ->                                  // exception → error hop
                followHops(graph, stepId, HopType.ERROR, ctx.with("error", e.getMessage()))
            );
    }

    private Mono<StepResult> followHops(JobGraph graph, String stepId,
                                         HopType type, JobContext ctx) {
        List<String> nextSteps = graph.getHops(stepId, type);

        if (nextSteps.isEmpty()) {
            return Mono.just(StepResult.success(ctx));           // end of chain
        }
        if (nextSteps.size() == 1) {
            return executeFromStep(graph, nextSteps.get(0), ctx); // sequential
        }

        // Parallel fan-out → Mono.zip
        List<Mono<StepResult>> parallel = nextSteps.stream()
            .map(next -> executeFromStep(graph, next, ctx))
            .toList();
        return Mono.zip(parallel, results -> mergeResults(results, ctx));
    }
}
```

### 5.4 Example Step Components (of the 85)

```java
// ── Database ──────────────────────────────────────────────────────
@Component
public class SqlQueryStep implements StepExecutor {
    private final DataSource dataSource;

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

// ── REST Call (natively non-blocking) ────────────────────────────
@Component
public class HttpPostStep implements StepExecutor {
    private final WebClient webClient;

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

// ── Shell Command ────────────────────────────────────────────────
@Component
public class ShellCommandStep implements StepExecutor {

    @Override
    public Mono<StepResult> execute(JobContext ctx) {
        return Mono.fromCallable(() -> {
            Process p = new ProcessBuilder("bash", "-c", ctx.param("command")).start();
            int exitCode = p.waitFor();
            return exitCode == 0
                ? StepResult.success(ctx)
                : StepResult.failure(ctx, "Exit code: " + exitCode);
        }).subscribeOn(Schedulers.boundedElastic());
    }
}

// ── Run Transformation (bridge to plain Java) ────────────────────
@Component
public class RunTransformationStep implements StepExecutor {
    private final TransformationPipeline pipeline;

    @Override
    public Mono<StepResult> execute(JobContext ctx) {
        return Mono.fromCallable(() -> {
            pipeline.execute(ctx.param("transformDef"), ctx.toTransformContext());
            return StepResult.success(ctx);
        }).subscribeOn(Schedulers.boundedElastic());
    }
}

// ── Mail ──────────────────────────────────────────────────────────
@Component
public class MailStep implements StepExecutor {
    private final JavaMailSender mailSender;

    @Override
    public Mono<StepResult> execute(JobContext ctx) {
        return Mono.fromCallable(() -> {
            mailSender.send(buildMessage(ctx));
            return StepResult.success(ctx);
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
```

### 5.5 What Gets Auto-Generated Per .kjb (Pure Data)

```java
/**
 * Auto-generated from: customer_daily_load.kjb
 * This class is ONLY data — no execution logic.
 * The DagExecutor runs it.
 */
@Component
public class CustomerDailyLoadJob implements JobDefinition {

    @Autowired SqlQueryStep sqlQuery;
    @Autowired RunTransformationStep runTransform;
    @Autowired SqlExecuteStep sqlExecute;
    @Autowired MailStep mail;

    @Override
    public JobGraph define(JobContext ctx) {
        return JobGraph.builder()
            .step("start",     Steps.start())
            .step("extract",   sqlQuery.with(ctx.param("source_query")))
            .step("transform", runTransform.with("customer_transform_v2"))
            .step("load",      sqlExecute.with(ctx.param("insert_query")))
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

### 5.6 DataRow (for Transformation Engine)

```java
public interface DataRow {
    Object get(String field);
    long getByteOffset();                  // for IndexSortOperator
    DataRow with(String field, Object value);
    DataRow without(String field);
    Map<String, Object> toMap();
    Set<String> fieldNames();
}
```

### 5.7 RowTransformer — Plain Java (for transformation steps)

```java
public interface RowTransformer {
    Iterator<DataRow> apply(Iterator<DataRow> input, TransformContext context);
}
```

### 5.8 TransformationPipeline — Plain Java (builds Iterator chain)

```java
public class TransformationPipeline {

    public TransformResult execute(TransformationDefinition def, TransformContext ctx) {
        Iterator<DataRow> pipeline = sourceStep.read(ctx);
        for (RowTransformer step : transformSteps) {
            pipeline = step.apply(pipeline, ctx);   // lazy — nothing executes yet
        }
        sinkStep.write(pipeline, ctx);              // pulls rows through the chain
        return new TransformResult(ctx.getMetrics());
    }
}
```

---

## 6. Automation Engine: Pentaho XML → JobDefinition Classes

The code generator produces **pure data classes** (JobGraph declarations) that
reference the 85 step components. No execution logic is generated.

### 6.1 Pipeline

```
.kjb files (1,500)
      │
      ▼
┌──────────────┐    ┌────────────────┐    ┌──────────────────────────────┐
│ Pentaho XML  │───►│ Intermediate   │───►│ Java Source Code Generator    │
│ Parser       │    │ Representation │    │ (produces JobDefinition class │
└──────────────┘    │ (IR)           │    │  with JobGraph.builder()...)  │
                    └────────────────┘    └──────────────────────────────┘
                                                    │
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

### 6.3 Code Generator: Pentaho Step Type → Step Component Mapping

The generator maps each Pentaho step type to one of the 85 step components:

| Pentaho Step Type (.kjb) | Generated Code (references step component) |
|---|---|
| `START` | `Steps.start()` |
| `SQL` | `sqlExecuteStep.with(query)` |
| `HTTP` | `httpPostStep.with(url, payload)` |
| `MAIL` | `mailStep.with(to, subject, body)` |
| `SHELL` | `shellCommandStep.with(command)` |
| `SFTP_PUT` | `sftpPutStep.with(host, path)` |
| `ABORT` | `Steps.abort(message)` |
| `SUCCESS` | `Steps.success()` |
| `JOB` (sub-job) | `runJobStep.with(subJobDef)` |
| `TRANS` (sub-trans) | `runTransformationStep.with(transformDef)` |
| `EVAL` | `evalConditionStep.with(expression)` |
| `SET_VARIABLES` | `setVariableStep.with(varName, value)` |

The generator also emits hop wiring:

| Pentaho Hop | Generated Code |
|---|---|
| OK hop | `.hop("stepA", "stepB", HopType.OK)` |
| Error hop | `.hop("stepA", "errorHandler", HopType.ERROR)` |
| Unconditional hop | `.hop("stepA", "stepB", HopType.UNCONDITIONAL)` |

### 6.4 The 85 Step Components: Build Priority

With 85 distinct step types identified, build in priority order:

| Priority | Step Types | Count | Coverage |
|---|---|---|---|
| **P1 — Build first** | SqlQuery, SqlExecute, RunTransformation, RunJob, Start, Success, Mail, Shell, SetVariable, FileCopy, SftpGet, SftpPut, HttpPost, Abort, Dummy | ~15 | ~80% of all jobs |
| **P2 — Build second** | StoredProc, BulkLoad, TableExists, CheckDbConn, FileExists, FileDelete, ExcelRead, KafkaProduce, S3Upload, SshExec, Delay, WaitForFile, EvalCondition, WriteToLog, etc. | ~35 | ~95% of all jobs |
| **P3 — Build last** | Remaining rare/specialized steps | ~35 | 100% |
| **Escape hatch** | Any unsupported step → `GroovyScriptStep` wrapping original logic | 1 | Fallback for anything |

### 6.5 Handling the Long Tail

1. **Audit first**: Parse all 1,500 .kjb files, count step type usage by frequency
2. **80/20 rule**: The top ~15 step types will cover ~80% of all jobs
3. **Build the 15 P1 components first** — unblocks bulk of migration
4. **Scripting escape hatch**: For rare steps not yet built, generate a `GroovyScriptStep`
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

- [ ] Set up Spring Boot project (WebFlux for API, standard for engines)
- [ ] Implement `StepExecutor` @FunctionalInterface + `StepResult` record
- [ ] Implement `JobDefinition` + `JobGraph` (graph model with hops)
- [ ] Implement `DagExecutor` (common engine: walks graph, chains Monos)
- [ ] Implement `JobContext` (immutable map, variable resolution)
- [ ] Implement `TransformationPipeline` (plain Java Iterator chain)
- [ ] Implement `DataRow` + `RowTransformer` (transformation abstractions)
- [ ] Build connector layer: JDBC/HikariCP, WebClient, File I/O
- [ ] Build observability: Micrometer metrics, OpenTelemetry tracing, SLF4J logging
- [ ] Build the P1 step components (~15): SqlQuery, RunTransformation, Mail, Shell, etc.
- [ ] Integration test framework with embedded DB and mock servers

### Phase 3: Code Generator (Weeks 6-10)

- [ ] Build code generator: IR → `JobDefinition` Java source files
- [ ] Map each Pentaho step type → appropriate step component reference
- [ ] Emit `JobGraph.builder()` with all step + hop declarations
- [ ] Handle sub-job references (`RunJobStep`) and sub-transformation references (`RunTransformationStep`)
- [ ] Handle parameter/variable substitution in step configs
- [ ] Output: compilable Spring `@Component` classes (pure data, no logic)
- [ ] Validation: compile check + verify all referenced step components exist
- [ ] Build P2 step components (~35) as needed by generated jobs

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

| Decision | Options | **Decision** |
|---|---|---|
| **Orchestration framework** | Reactor everywhere vs Reactor+plain Java hybrid | **Hybrid**: Reactor `Mono` for orchestration (.kjb), plain Java `Iterator` for transforms (.ktr) |
| **DB access (transforms)** | R2DBC vs JDBC | **JDBC + HikariCP** — transforms are plain Java, no reactive wrappers needed |
| **DB access (orchestration)** | R2DBC vs JDBC-on-elastic | JDBC-on-elastic (broader driver support; R2DBC optional for high-concurrency steps) |
| **Row representation** | Generic Map vs typed POJOs | Generic `DataRow` (Map-backed) for flexibility; typed POJOs for high-frequency paths |
| **Job definition format** | Pure Java code vs external DSL/YAML | Java code (type-safe, IDE support, testable, debuggable) |
| **Scheduling** | Spring `@Scheduled` vs Quartz vs external | External scheduler triggering via REST API (decouples scheduling from execution) |
| **Error handling** | Fail-fast vs configurable retry | Configurable per-step: retry count, backoff, dead-letter |
| **State management** | Stateless vs checkpoint/resume | Stateless first; add checkpointing for long-running jobs later |
| **Script steps** | GraalVM polyglot vs Groovy | Groovy (mature Spring integration, familiar to Java devs) |
| **Testing (transforms)** | StepVerifier vs plain JUnit | **Plain JUnit assertions** — no reactive test complexity needed |
| **Testing (orchestration)** | StepVerifier vs plain JUnit | StepVerifier for Mono chains |
| **Testing (E2E)** | Parity testing | Testcontainers + output comparison vs Pentaho |

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

### Strategy: Lightweight Record Index Sort (Primary — No Disk I/O)

Instead of sorting full rows in memory (OOM) or spilling to disk (slow I/O),
we use **Java Records with only the sort key(s) + file byte offset**. This gives
us a 10-15x memory reduction, keeping the sort entirely in-heap for most datasets.

#### Memory comparison (2GB CSV, ~50 columns, ~4M rows)

| Approach | Per-row memory | Total (4M rows) | Disk I/O |
|---|---|---|---|
| Full row sort | ~500 bytes | **~2GB** (OOM) | None |
| **Record index sort** | **~30-50 bytes** | **~120-200MB** (fits heap) | **1 extra sequential read** |
| External disk sort | ~0 in heap | ~0 | **Heavy random I/O** |

#### How it works: Two-pass sort

```
Pass 1: Build sorted index (single sequential read)
┌──────────────────────────────────────────────────────────────────────┐
│  CSV File (2GB)                                                      │
│  offset=0      → "Alice,Engineering,95000,NYC,..."                   │
│  offset=1042   → "Bob,Sales,72000,Chicago,..."                       │
│  offset=2089   → "Charlie,Engineering,88000,NYC,..."                 │
│  ...                                                                 │
└──────────┬───────────────────────────────────────────────────────────┘
           │
           ▼  Extract sort key + byte offset
┌──────────────────────────────┐
│  List<SortEntry> (in heap)   │
│  record(key="Engineering",   │
│         salary=95000,        │
│         offset=0)            │  ← ~40 bytes per entry
│  record(key="Sales",         │
│         salary=72000,        │
│         offset=1042)         │
│  ...                         │
│  4M entries × 40 bytes       │
│  = ~160MB (fits in heap)     │
└──────────┬───────────────────┘
           │
           ▼  Arrays.parallelSort() — in-memory, no disk
┌──────────────────────────────┐
│  Sorted SortEntry[]          │
│  (sorted by key, salary)     │
└──────────────────────────────┘

Pass 2: Emit full rows in sorted order (sequential or random access read)
┌──────────────────────────────┐
│  For each sorted entry:      │
│    seek(entry.offset)        │
│    read full row             │
│    emit as DataRow           │
│                              │
│  → Flux<DataRow> (sorted)    │
└──────────────────────────────┘
```

#### Core design: `SortEntry` Record

```java
/**
 * Lightweight record holding ONLY the sort key(s) and the byte offset
 * of the full row in the source CSV file. Typically 30-50 bytes per entry
 * vs 500+ bytes for a full DataRow.
 */
public record SortEntry(
    Object[] sortKeys,      // only the fields needed for comparison
    long byteOffset         // position in source file to read full row
) implements Comparable<SortEntry> {

    @Override
    public int compareTo(SortEntry other) {
        // Compare sortKeys field by field using configured sort order
    }
}
```

#### Design: `IndexSortOperator` (Plain Java)

```java
public class IndexSortOperator implements RowTransformer {

    private final List<SortField> sortFields;
    private final Path sourceFile;

    @Override
    public Iterator<DataRow> apply(Iterator<DataRow> input, TransformContext context) {
        // Pass 1: Build sorted index — lightweight records only
        List<SortEntry> entries = new ArrayList<>();
        while (input.hasNext()) {
            DataRow row = input.next();
            entries.add(new SortEntry(extractSortKeys(row), row.getByteOffset()));
        }

        // In-memory sort — 40 bytes/entry, fits in heap for millions of rows
        SortEntry[] array = entries.toArray(SortEntry[]::new);
        Arrays.parallelSort(array);

        // Pass 2: Return iterator that reads full rows in sorted order
        return new SortedRowIterator(array, sourceFile);
    }
}

/**
 * Lazily reads full rows from the CSV file using byte offsets.
 * Only one row is in memory at a time during iteration.
 */
class SortedRowIterator implements Iterator<DataRow>, AutoCloseable {
    private final SortEntry[] sortedEntries;
    private final RandomAccessFile raf;
    private int index = 0;

    SortedRowIterator(SortEntry[] sortedEntries, Path sourceFile) {
        this.sortedEntries = sortedEntries;
        this.raf = new RandomAccessFile(sourceFile.toFile(), "r");
    }

    @Override public boolean hasNext() { return index < sortedEntries.length; }

    @Override
    public DataRow next() {
        SortEntry entry = sortedEntries[index++];
        raf.seek(entry.byteOffset());
        return parseLine(raf.readLine());
    }

    @Override public void close() { closeQuietly(raf); }
}
```

#### Optimizing Pass 2: Sequential vs Random Access

| Strategy | When to use |
|---|---|
| **RandomAccessFile seek** | SSD storage (seek is ~0.1ms) — works for any order |
| **Batch + sequential re-read** | HDD — group offsets into page-aligned chunks, read sequentially |
| **Memory-mapped file** | `MappedByteBuffer` via `FileChannel.map()` — OS handles caching |

### Design: Streaming CSV Reader (Plain Java)

The CSV reader must track **byte offsets** so the sort index can reference rows.

```java
public class CsvReader {

    /**
     * Reads a CSV file as an Iterator<DataRow>, streaming line by line.
     * Each DataRow carries its byte offset in the source file.
     * Memory: O(1) — one row at a time.
     */
    public Iterator<DataRow> read(Path csvFile, CsvConfig config) {
        OffsetTrackingReader reader = new OffsetTrackingReader(csvFile, config.charset());
        if (config.hasHeader()) reader.skipHeader();
        return new CsvRowIterator(reader, config);
    }
}
```

### Memory Budget & Thresholds

| Parameter | Default | Notes |
|---|---|---|
| `sort.record-overhead` | 40 bytes | Estimated memory per SortEntry record |
| `sort.max-heap-budget` | 1GB | Max heap allowed for sort index |
| `sort.max-index-rows` | 25,000,000 | = budget / overhead; beyond this, fall back to external sort |
| `sort.parallel-sort` | `true` | Use `Arrays.parallelSort()` for multi-core |
| `sort.pass2-strategy` | `auto` | `mmap` for SSD, `batch-sequential` for HDD |
| `csv.buffer-size` | 8KB | BufferedReader buffer |

### Decision Logic: Three-Tier Sort Strategy

```java
public RowTransformer createSortStep(SortConfig config, long estimatedRows) {
    long indexMemory = estimatedRows * config.getRecordOverhead();

    if (estimatedRows < 500_000) {
        // Tier 1 — Small: full in-memory sort
        return (input, ctx) -> {
            List<DataRow> all = collectToList(input);
            all.sort(comparator);
            return all.iterator();
        };

    } else if (indexMemory < config.getMaxHeapBudget()) {
        // Tier 2 — Large: Record index sort (no disk I/O, ~10-15x less memory)
        return new IndexSortOperator(config);

    } else {
        // Tier 3 — Extreme (25M+ rows): external merge sort (disk-backed fallback)
        return new ExternalSortOperator(config);
    }
}
```

### Streaming Operators — Plain Java (NOT Blocking)

All operate on sorted `Iterator<DataRow>` input. O(1) memory. No Reactor types.

#### Streaming Group By

```java
public class StreamingGroupByOperator implements RowTransformer {

    @Override
    public Iterator<DataRow> apply(Iterator<DataRow> sortedInput, TransformContext ctx) {
        // Returns an iterator that:
        // - Reads consecutive rows with same group key
        // - Aggregates them (sum, count, min, max, etc.)
        // - Emits one result row per group
        // Memory: holds only ONE group at a time
        return new GroupByIterator(sortedInput, groupKey, aggregations);
    }

    // For unsorted input with LOW cardinality:
    public Iterator<DataRow> applyUnsorted(Iterator<DataRow> input, TransformContext ctx) {
        Map<Object, Accumulator> accumulators = new HashMap<>();
        while (input.hasNext()) {
            DataRow row = input.next();
            accumulators.computeIfAbsent(row.get(groupKey), k -> new Accumulator())
                        .add(row);
        }
        return accumulators.values().stream()
            .map(Accumulator::toRow)
            .iterator();
        // Memory: O(G) where G = number of distinct groups (must be small)
    }
}
```

#### Streaming Dedupe (Unique Rows)

```java
public class StreamingDedupeOperator implements RowTransformer {

    @Override
    public Iterator<DataRow> apply(Iterator<DataRow> sortedInput, TransformContext ctx) {
        return new Iterator<>() {
            private DataRow next = advance();
            private Object lastKey = null;

            private DataRow advance() {
                while (sortedInput.hasNext()) {
                    DataRow row = sortedInput.next();
                    Object key = row.get(dedupeKey);
                    if (!Objects.equals(key, lastKey)) {
                        lastKey = key;
                        return row;
                    }
                }
                return null;
            }

            @Override public boolean hasNext() { return next != null; }
            @Override public DataRow next() {
                DataRow current = next;
                next = advance();
                return current;
            }
        };
        // Memory: O(1) — only holds previous key for comparison
    }
}
```

#### Streaming Merge Join

```java
public class StreamingMergeJoinOperator {

    public Iterator<DataRow> join(Iterator<DataRow> left, Iterator<DataRow> right,
                                   String joinKey) {
        // Both inputs pre-sorted on joinKey
        // Classic merge join: advance two pointers, emit matches
        // Memory: O(1) per pair
        return new MergeJoinIterator(left, right, joinKey);
    }
}
```

### Key Principles

> 1. **Reactor for orchestration (.kjb), plain Java for transformation (.ktr).**
> 2. **Sort is the only truly blocking operation.** Everything else streams via Iterator.
> 3. **Record index sort** (lightweight) is the primary strategy — external sort is fallback only.
> 4. **All other operators chain on sorted Iterator output** — O(1) memory per row.
> 5. **The bridge**: `RunTransformationStep` wraps plain Java in `Mono.fromCallable()`.
> 6. **Debugging is simple**: transformation stack traces are normal Java; only orchestration uses reactive.

---

## 11. Success Metrics

- **Automation rate**: % of jobs fully auto-generated (target: >85%)
- **Parity pass rate**: % of jobs producing identical output to Pentaho (target: >99%)
- **Performance**: P95 latency and throughput equal or better than Pentaho
- **Operational**: All jobs observable via metrics/traces/logs
- **Timeline**: Full cutover within 24 weeks

---

## 12. Technology Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21+, Spring Boot 3.x |
| Orchestration (.kjb) | Spring WebFlux + Project Reactor (Mono) |
| Transformation (.ktr) | Plain Java (Iterator/Stream, no reactive types) |
| Admin API | Spring WebFlux (REST endpoints, SSE monitoring) |
| Database (transforms) | JDBC + HikariCP (plain, blocking — simple) |
| Database (orchestration) | WebClient or JDBC via `Schedulers.boundedElastic()` |
| HTTP | Spring WebClient (orchestration), `HttpURLConnection`/OkHttp (transforms) |
| Messaging | Reactor Kafka (orchestration), Kafka client (transforms) |
| File I/O | `BufferedReader` / `RandomAccessFile` / `MappedByteBuffer` (plain Java) |
| Excel | Apache POI (streaming SXSSF for large files) |
| Scripting | Groovy (via GroovyShell) |
| Observability | Micrometer + Prometheus, OpenTelemetry, SLF4J + Logback |
| Testing (orchestration) | JUnit 5, reactor-test StepVerifier |
| Testing (transforms) | JUnit 5, plain assertions (no StepVerifier needed) |
| Testing (integration) | Testcontainers |
| Build | Maven multi-module |
| Code Gen | JavaPoet or Roaster for Java source generation |
| Pentaho Parsing | JAXB or Jackson XML for .kjb/.ktr parsing |
