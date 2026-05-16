# S3 Read Benchmark Performance Analysis

## Overview

This document summarizes the performance observations from testing different request allocation strategies in the S3 Read Benchmark tool using Q9 trace data.

## Test Configuration

- **Dataset**: Q9 trace (requests from different files mixed together, completely out of order)
- **Metric**: Maximum throughput (MB/s or GB/s)
- **Test Scenarios**: Various combinations of sorting and thread allocation strategies

## Performance Results

| Strategy | Sorting | Allocation Method | Throughput | Performance |
|----------|---------|-------------------|------------|-------------|
| 1 | None (original order) | Partitioned by thread | 700 MB/s | Baseline |
| 2 | None (original order) | Round-robin | 700 MB/s | No improvement |
| 3 | **Sorted by file** | Partitioned by thread | **1.4 GB/s** | **2x improvement** ✓ |
| 4 | **Sorted by file** | Round-robin | 1.1 GB/s | 1.57x improvement |
| 5 | **Sorted by file** | **Smart round-robin*** | **1.4 GB/s** | **2x improvement** ✓ |

\* Smart round-robin: Consecutive requests for the same file stay in the same thread, round-robin allocation for different files

## Key Findings

### 1. Sorting is Critical (2x Performance Gain)

The most significant performance improvement comes from **sorting operations by file** before allocation:
- **Without sorting**: 700 MB/s (regardless of allocation method)
- **With sorting**: 1.1-1.4 GB/s (depending on allocation method)

**Why sorting matters:**
- Improves cache locality for file metadata and connection reuse
- Reduces S3 connection overhead by batching requests to the same file
- Enables better HTTP connection pooling and keep-alive optimization

### 2. Allocation Strategy Matters (After Sorting)

Once operations are sorted by file, the allocation strategy affects performance:

#### Best Performers (1.4 GB/s):
- **Simple partitioning**: Divide sorted operations into contiguous blocks per thread
- **Smart round-robin**: Keep same-file requests together, round-robin for different files

#### Moderate Performer (1.1 GB/s):
- **Pure round-robin**: Distributes sorted operations cyclically across threads
  - Still benefits from sorting but breaks up some same-file request sequences

### 3. Without Sorting, Allocation Doesn't Matter

When operations are unsorted (original Q9 trace order):
- Partitioned allocation: 700 MB/s
- Round-robin allocation: 700 MB/s
- **Conclusion**: Allocation strategy has no impact on unsorted data

## Recommendations

### Optimal Configuration (Implemented)

The benchmark now implements **Strategy #5** (sorted + smart round-robin):

```cpp
// 1. Sort operations by file
std::sort(operations, [](a, b) { return a.file < b.file; });

// 2. Allocate with smart round-robin
for each operation:
    if same_file_as_previous:
        assign_to_same_thread()
    else:
        assign_to_next_thread_round_robin()
```

**Benefits:**
- Achieves maximum throughput (1.4 GB/s)
- Maintains good load balancing across threads
- Keeps related file operations together for optimal caching

### Alternative: Simple Partitioning

For simpler implementation, **Strategy #3** (sorted + partitioned) also achieves 1.4 GB/s:

```cpp
// 1. Sort operations by file
std::sort(operations, [](a, b) { return a.file < b.file; });

// 2. Divide into equal chunks per thread
chunk_size = total_ops / num_threads
for each thread:
    assign operations[start:end]
```

## Performance Impact Summary

| Factor | Impact | Magnitude |
|--------|--------|-----------|
| **Sorting by file** | Critical | **+100% throughput** |
| Allocation strategy (with sorting) | Moderate | 0-27% variation |
| Allocation strategy (without sorting) | None | 0% variation |

## Conclusion

**Sorting operations by file is the single most important optimization**, providing a 2x throughput improvement. The allocation strategy provides additional optimization opportunities, with smart round-robin and simple partitioning both achieving optimal results when combined with sorting.

The current implementation uses sorted + smart round-robin allocation to achieve maximum throughput of 1.4 GB/s on Q9 trace data.