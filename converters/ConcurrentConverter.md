# Assessment: Concurrent Converter Upgrades

## Executive Summary
Recent upgrades to the `FilesystemConverter` introduced robust concurrency (Worker Pools) and safety mechanisms (Timeout/Cancellation) to handle large directory scans and potential I/O hangs. This assessment evaluates the applicability of these patterns to other converters (`csv`, `json`, `txt`, `zip`).

**Conclusion:** While the **Worker Pool** pattern is largely specific to the hierarchical/independent nature of filesystem scanning, the **Timeout and Cancellation** mechanisms are universally applicable and highly recommended for all stream-based converters to prevent indefinite blocking.

---

## 1. Analysis of Filesystem Upgrades

The `FilesystemConverter` received the following enhancements:

1.  **Worker Pool Concurrency (32 Workers)**
    *   **Mechanism:** Uses a buffered channel of jobs (paths) and a pool of goroutines to `ReadDir` and `Stat` files in parallel.
    *   **Benefit:** Drastically reduces scan time for large directory trees by mitigating I/O latency.
    
2.  **Idle Timeout Safety**
    *   **Mechanism:** A monitor goroutine resets a timer whenever a result is yielded. If no activity occurs for `timeout` duration, the scan is halted.
    *   **Benefit:** Prevents the application from hanging indefinitely if a mount becomes unresponsive or a read blocks forever.

3.  **Permission Retry Logic**
    *   **Mechanism:** Specific handling for `fs.ErrPermission` with a backoff-retry loop.
    *   **Benefit:** Increases resilience in transient locking scenarios.

4.  **Resumption**
    *   **Mechanism:** Skips paths lexicographically less than a provided `ResumePath`.
    *   **Benefit:** Allows restarting long scans without re-processing everything.

---

## 2. Applicability Assessment by Converter

### A. CSV, TXT, JSON (Stream-Based Converters)

**Current State:**
*   These converters typically wrap a core `io.Reader` (or `bufio.Scanner` / `csv.Reader` / `json.Decoder`).
*   Processing is inherently sequential: byte N must be read before byte N+1.
*   `csv.go` and others use a simple Producer-Consumer pattern (one goroutine reading/parsing, main thread yielding) to pipeline I/O and processing.

**Applicability of Upgrades:**

| Feature | Applicability | Analysis |
| :--- | :--- | :--- |
| **Worker Pool** | **Low** | Since the input is a single serial stream, we cannot easily split the work among 32 workers without complex offsets or indexing. Parallelizing the *parsing* of lines is possible but often adds synchronization overhead that outweighs the benefit for IO-bound tasks. |
| **Idle Timeout** | **High** | **Critical.** If the underlying `io.Reader` (e.g., a network socket or a piped process) hangs, `csv.Read()` will block forever. Implementing the `IdleTimeout` monitor found in `FilesystemConverter` is strongly recommended. |
| **Resumption** | **Medium** | "Seek to line N" is possible but expensive without an index. Resumption is better handled at the file level (by the caller) rather than within the file parser. |

### B. ZIP (Archive Converter)

**Current State:**
*   Uses `archive/zip`.
*   **Fast Path:** If input is `ReaderAt` (File), it reads the Central Directory at the end.
*   **Slow Path:** If input is a stream, it copies to a temp file, then reads Central Directory.

**Applicability of Upgrades:**

| Feature | Applicability | Analysis |
| :--- | :--- | :--- |
| **Worker Pool** | **Low** | The Central Directory is a linear list of file headers. Iterating this list is extremely fast (CPU bound). Parallelism is unnecessary for *listing* files. (Note: If we were *extracting* content, parallelism would be High). |
| **Idle Timeout** | **High** | The "Slow Path" (Copying stream to temp file) is a blocking operation that reads the entire stream. If the source stalls, the converter hangs. A timeout wrapper around the copy operation is needed. |

---

## 3. Recommendations

### 1. Universal Adoption of "Idle Timeout"
The "Watchdog" pattern implemented in `filesystem.go` should be abstracted and applied to `csv`, `json`, `txt`, and `zip`.

**Proposal:**
Create a wrapper helper in `common` or a reusable utility:
```go
// common/timeout.go
func Watchdog(ctx context.Context, timeout time.Duration, yield func() error)
```
*   **Why:** It ensures no converter can silently hang the entire application, which is crucial for server stability.

### 2. Standardization of Cancellation
All converters should respect a `doneCh` or `context.Context` to allow the user to abort a long conversion (e.g., a 10GB CSV import).
*   `filesystem.go` uses `doneCh`.
*   `csv.go` uses a limited error channel.
*   **Action:** Update `RowProvider.ScanRows` to possibly accept a Context, or ensure the `yield` function return value (error) is correctly handled to stop processing immediately (which `csv` seems to do).

### 3. "Multi-File" Parallel Converter
*Rejected.* The user has decided that multi-file orchestration and parallelization should be the responsibility of a higher-level application or a different tool using `mksqlite`, rather than being built into the low-level converters themselves.

## Summary Checklist for Next Steps

*   [ ] **Refactor Timeout:** Extract the timeout/watchdog logic from `filesystem.go` into `common`.
*   [ ] **Apply Timeout:** Wrap `csv.go`, `json.go`, `txt.go` loops with the watchdog.
*   [ ] **Zip Safety:** Add timeout context to the `io.Copy` in `ZipConverter`'s stream fallback.
