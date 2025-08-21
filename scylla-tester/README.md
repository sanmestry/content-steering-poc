**Purpose:**  
Simulate high-throughput writes and periodic read queries against a ScyllaDB table (`content_sessions`) to measure ingest rate and query latency.

**Key Components:**

1. **Configuration:**
    - Test duration (`runDuration`), number of write workers (`workerCount`), read query interval (`readInterval`), keyspace, and table name are set as constants.
    - CDN names, platforms, and ASNs for queries are defined as slices.

2. **SessionData Struct:**
    - Represents a row to be inserted: session ID (UUID), timestamp, platform, ASN, CDN, and video profile bitrate.

3. **Main Function:**
    - Reads ScyllaDB connection details from environment variables.
    - Initializes a ScyllaDB session using `gocql`.
    - Sets up a context with a timeout for the test duration.
    - Launches multiple write workers (goroutines) and a single read worker.
    - Waits for all workers to finish.
    - Calculates and logs total writes and average ingest rate.

4. **Write Worker:**
    - Each worker repeatedly generates random `SessionData` and inserts it into ScyllaDB.
    - Uses an atomic counter to track successful writes.
    - Logs errors but does not retry failed writes.
    - Sleeps briefly between writes to control throughput.

5. **Read Worker:**
    - Periodically (every `readInterval`) runs an aggregation query for a random ASN from the predefined list.
    - Aggregates total traffic per CDN for the last 24 hours.
    - Logs query latency, number of scanned rows, and aggregated results.
    - Cleans up resources by stopping the ticker when done.

**Concurrency:**
- Uses goroutines for parallelism and a `sync.WaitGroup` to wait for all workers.
- Atomic operations ensure thread-safe write counting.

**Resource Management:**
- Context cancellation stops all workers after the test duration.
- Database session and ticker are properly closed.

**Metrics:**
- Tracks total writes and calculates average writes per second.
- Logs read query latency and aggregation results.

**Summary:**  
The file is designed for benchmarking ScyllaDB write and read performance under concurrent load, with configurable parameters and basic error logging.