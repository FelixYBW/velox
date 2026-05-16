# S3 Read Benchmark

This benchmark tool reproduces S3 read operations from trace logs in multiple threads to measure performance and throughput.

## Overview

The S3ReadBenchmark tool parses trace logs containing S3 GetObject operations and replays them using configurable number of threads. This is useful for:
- Performance testing of S3 read operations
- Reproducing production workload patterns
- Measuring throughput and latency under different thread configurations
- Identifying bottlenecks in S3 read paths

## Building

The benchmark is built as part of the Velox build process. It's built independently of the testing flag, so it works even with `-DVELOX_BUILD_TESTING=OFF`.

### Standard Build

```bash
cd /velox
mkdir -p _build && cd _build
cmake .. -DVELOX_ENABLE_S3=ON
make velox_s3read_benchmark
```

### Build with Custom Flags

```bash
cd /velox
make velox_s3read_benchmark NUM_THREADS=14 MAX_HIGH_MEM_JOBS=14 MAX_LINK_JOBS=14 \
  'EXTRA_CMAKE_FLAGS=-DVELOX_ENABLE_S3=ON -DVELOX_BUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release'
```

**Note**: You need to run `make cmake` first if this is a fresh build or if CMakeLists.txt has changed:

```bash
cd /velox
make cmake EXTRA_CMAKE_FLAGS='-DVELOX_ENABLE_S3=ON'
make velox_s3read_benchmark
```

## Trace Log Format

The benchmark expects trace logs in the following format:

```
GetObject bucket=<bucket-name> key=<object-key> range=bytes=<start>-<end> file=s3://<bucket-name>/<object-key>
```

Example:
```
GetObject bucket=adobe-workload-east-1 key=xdm/62b89f2ae63a221b63b2f6c1/_ACP_DATE=2020-01-29/_ACP_BATCHID=01G6GN2E24FARAA1A456Y3W75D/part-01022-17ef7759-0c4e-409a-a9ee-28ff48397539.c000.snappy.parquet range=bytes=86250071-86377805 file=s3://adobe-workload-east-1/xdm/62b89f2ae63a221b63b2f6c1/_ACP_DATE=2020-01-29/_ACP_BATCHID=01G6GN2E24FARAA1A456Y3W75D/part-01022-17ef7759-0c4e-409a-a9ee-28ff48397539.c000.snappy.parquet
```

A sample trace log file is provided at `sample_trace.log`.

## Usage

### Basic Usage

```bash
./velox_s3read_benchmark --trace_log_file=/path/to/trace.log
```

### With Custom Thread Count

```bash
./velox_s3read_benchmark --trace_log_file=/path/to/trace.log --num_threads=8
```

### Multiple Iterations

```bash
./velox_s3read_benchmark --trace_log_file=/path/to/trace.log --num_threads=4 --iterations=3
```

### Verbose Mode

```bash
./velox_s3read_benchmark --trace_log_file=/path/to/trace.log --verbose=true
```

## Command Line Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--trace_log_file` | string | (required) | Path to trace log file containing GetObject operations |
| `--num_threads` | int | 4 | Number of threads to use for parallel reads |
| `--iterations` | int | 1 | Number of times to run the benchmark |
| `--verbose` | bool | false | Enable verbose output showing each read operation |

## Output

The benchmark provides the following metrics:

1. **Summary Statistics**:
   - Total number of operations
   - Number of unique files
   - Total bytes to read
   - Operations per file

2. **Performance Metrics** (per iteration):
   - Total execution time (milliseconds)
   - Total bytes read
   - Throughput (MB/s)
   - Operations per second

### Example Output

```
I0516 07:24:00.000 Loaded 100 S3 read operations from trace log
I0516 07:24:00.001 === Trace Log Summary ===
I0516 07:24:00.001 Total operations: 100
I0516 07:24:00.001 Unique files: 5
I0516 07:24:00.001 Total bytes to read: 524288000 (500.0 MB)
I0516 07:24:00.001 
I0516 07:24:00.001 Operations per file:
I0516 07:24:00.001   s3://bucket/file1.parquet: 20 operations, 100.0 MB
I0516 07:24:00.001   s3://bucket/file2.parquet: 30 operations, 150.0 MB
...
I0516 07:24:00.002 Starting benchmark with 4 threads, 1 iterations, 100 operations
I0516 07:24:00.002 Iteration 1/1
I0516 07:24:00.002 Thread 0 processing operations [0, 25)
I0516 07:24:00.002 Thread 1 processing operations [25, 50)
I0516 07:24:00.002 Thread 2 processing operations [50, 75)
I0516 07:24:00.002 Thread 3 processing operations [75, 100)
I0516 07:24:05.123 Thread 0 completed
I0516 07:24:05.234 Thread 1 completed
I0516 07:24:05.345 Thread 2 completed
I0516 07:24:05.456 Thread 3 completed
I0516 07:24:05.456 Iteration 1 completed in 5454 ms
I0516 07:24:05.456 Total bytes read: 524288000 (500.0 MB)
I0516 07:24:05.456 Throughput: 91.67 MB/s
I0516 07:24:05.456 Operations per second: 18.34
I0516 07:24:05.456 Benchmark completed successfully
```

## Configuration

The benchmark uses the S3 configuration from your environment. Make sure to set up:

1. **AWS Credentials**: Set via environment variables or AWS config files
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_REGION=us-east-1
   ```

2. **S3 Endpoint** (if using custom S3-compatible storage):
   ```bash
   export AWS_ENDPOINT_URL=http://localhost:9000
   ```

## Thread Scaling

The benchmark distributes operations evenly across threads. For example, with 100 operations and 4 threads:
- Thread 0: operations 0-24
- Thread 1: operations 25-49
- Thread 2: operations 50-74
- Thread 3: operations 75-99

## Performance Tips

1. **Thread Count**: Start with a thread count matching your CPU cores, then experiment with higher values
2. **Network Bandwidth**: Ensure sufficient network bandwidth to S3
3. **S3 Configuration**: Tune S3 client settings (connection pool size, timeouts, etc.)
4. **File Locality**: Operations on the same file may benefit from caching

## Troubleshooting

### "Failed to open trace log file"
- Verify the file path is correct
- Check file permissions

### "Failed to execute read"
- Verify S3 credentials are configured
- Check network connectivity to S3
- Ensure the S3 bucket and objects exist
- Verify you have read permissions on the objects

### Low Throughput
- Increase thread count
- Check network bandwidth
- Verify S3 endpoint is optimal for your location
- Consider S3 transfer acceleration if available

## See Also

- [`S3ReadTest.cpp`](S3ReadTest.cpp) - Unit tests for S3 read operations
- [`S3FileSystem.h`](../S3FileSystem.h) - S3 filesystem implementation