/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <algorithm>
#include <fstream>
#include <numeric>
#include <regex>
#include <thread>

#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"

DEFINE_string(
    trace_log_file,
    "",
    "Path to trace log file containing GetObject operations");
DEFINE_int32(num_threads, 4, "Number of threads to use for benchmark");
DEFINE_int32(iterations, 1, "Number of iterations to run");
DEFINE_bool(
    verbose,
    false,
    "Enable verbose output showing each read operation");
DEFINE_bool(
    sort_by_file,
    true,
    "Sort operations by file before execution for optimal throughput");
DEFINE_bool(
    group_same_file,
    true,
    "Keep consecutive requests for the same file in the same thread");

namespace facebook::velox::filesystems {

struct S3ReadOperation {
  std::string bucket;
  std::string key;
  std::string file;
  int64_t rangeStart;
  int64_t rangeEnd;

  int64_t size() const {
    return rangeEnd - rangeStart + 1;
  }

  std::string toString() const {
    return fmt::format(
        "bucket={} key={} range=bytes={}-{} size={}",
        bucket,
        key,
        rangeStart,
        rangeEnd,
        size());
  }
};

class S3ReadBenchmark {
 public:
  S3ReadBenchmark() {
    // Initialize S3 filesystem
    filesystems::initializeS3("Info", "/tmp/s3_benchmark/");
    filesystems::registerS3FileSystem();
    
    // Load config from dump file if it exists
    std::unordered_map<std::string, std::string> configValues = loadConfigFromDump("/tmp/s3config_dump.txt");
    config_ = std::make_shared<const config::ConfigBase>(std::move(configValues));
  }

  ~S3ReadBenchmark() {
    filesystems::finalizeS3FileSystem();
    filesystems::finalizeS3();
  }

  // Parse trace log line to extract S3 read operation details
  // Format: GetObject timestamp=<timestamp> bucket=<bucket> key=<key> range=bytes=<start>-<end>
  std::optional<S3ReadOperation> parseTraceLogLine(const std::string& line) {
    static const std::regex pattern(
        R"(GetObject\s+timestamp=\d+\s+bucket=([^\s]+)\s+key=([^\s]+)\s+range=bytes=(\d+)-(\d+))");

    std::smatch matches;
    if (std::regex_search(line, matches, pattern)) {
      S3ReadOperation op;
      op.bucket = matches[1].str();
      op.key = matches[2].str();
      op.rangeStart = std::stoll(matches[3].str());
      op.rangeEnd = std::stoll(matches[4].str());
      // Construct the S3 file path from bucket and key
      op.file = fmt::format("s3://{}/{}", op.bucket, op.key);
      return op;
    }
    return std::nullopt;
  }

  // Load trace log file and parse all S3 read operations
  std::vector<S3ReadOperation> loadTraceLog(const std::string& filePath) {
    std::vector<S3ReadOperation> operations;
    std::ifstream file(filePath);

    if (!file.is_open()) {
      throw std::runtime_error(
          fmt::format("Failed to open trace log file: {}", filePath));
    }

    std::string line;
    int lineNum = 0;
    while (std::getline(file, line)) {
      lineNum++;
      auto op = parseTraceLogLine(line);
      if (op.has_value()) {
        operations.push_back(op.value());
      } else if (!line.empty() && FLAGS_verbose) {
        LOG(WARNING) << "Failed to parse line " << lineNum << ": " << line;
      }
    }

    LOG(INFO) << "Loaded " << operations.size()
              << " S3 read operations from trace log";
    return operations;
  }

  // Execute a single S3 read operation
  void executeRead(const S3ReadOperation& op) {
    try {
      auto fs = filesystems::getFileSystem(op.file, config_);
      auto readFile = fs->openFileForRead(op.file);

      // Allocate buffer for the read
      auto buffer = std::make_unique<char[]>(op.size());

      // Perform the range read
      auto result = readFile->pread(op.rangeStart, op.size(), buffer.get());
      auto bytesRead = result.size();

      if (FLAGS_verbose) {
        LOG(INFO) << "Read " << bytesRead << " bytes: " << op.toString();
      }

      if (bytesRead != op.size()) {
        LOG(WARNING) << "Expected to read " << op.size() << " bytes but got "
                     << bytesRead << " for " << op.toString();
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to execute read: " << op.toString()
                 << " Error: " << e.what();
    }
  }

  // Worker thread function that processes operations assigned via round-robin
  void workerThread(
      const std::vector<S3ReadOperation>& operations,
      const std::vector<size_t>& assignedIndices,
      int threadId) {
    LOG(INFO) << "Thread " << threadId << " processing "
              << assignedIndices.size() << " operations";

    for (size_t idx : assignedIndices) {
      executeRead(operations[idx]);
    }

    LOG(INFO) << "Thread " << threadId << " completed";
  }

  // Run benchmark with multiple threads
  void runBenchmark(
      const std::vector<S3ReadOperation>& operations,
      int numThreads,
      int iterations) {
    if (operations.empty()) {
      LOG(WARNING) << "No operations to execute";
      return;
    }

    LOG(INFO) << "Starting benchmark with " << numThreads << " threads, "
              << iterations << " iterations, " << operations.size()
              << " operations";
    LOG(INFO) << "Configuration: sort_by_file=" << FLAGS_sort_by_file
              << ", group_same_file=" << FLAGS_group_same_file;

    // Optionally sort operations by file to group requests for the same file together
    std::vector<size_t> sortedIndices(operations.size());
    std::iota(sortedIndices.begin(), sortedIndices.end(), 0);
    
    if (FLAGS_sort_by_file) {
      std::sort(sortedIndices.begin(), sortedIndices.end(),
                [&operations](size_t a, size_t b) {
                  return operations[a].file < operations[b].file;
                });
      LOG(INFO) << "Sorted operations by file for optimal throughput";
    } else {
      LOG(INFO) << "Using original operation order (no sorting)";
    }

    for (int iter = 0; iter < iterations; ++iter) {
      LOG(INFO) << "Iteration " << (iter + 1) << "/" << iterations;

      auto startTime = std::chrono::high_resolution_clock::now();

      // Assign operations to threads
      std::vector<std::vector<size_t>> threadAssignments(numThreads);
      int currentThread = 0;
      
      if (FLAGS_group_same_file) {
        // Keep consecutive requests for the same file in the same thread
        for (size_t i = 0; i < sortedIndices.size(); ++i) {
          size_t currentIdx = sortedIndices[i];
          
          // Check if this operation is for the same file as the previous one
          if (i > 0) {
            size_t prevIdx = sortedIndices[i - 1];
            if (operations[currentIdx].file == operations[prevIdx].file) {
              // Keep in the same thread as previous operation
              threadAssignments[currentThread].push_back(currentIdx);
            } else {
              // Different file - use round-robin
              currentThread = (currentThread + 1) % numThreads;
              threadAssignments[currentThread].push_back(currentIdx);
            }
          } else {
            // First operation
            threadAssignments[currentThread].push_back(currentIdx);
          }
        }
      } else {
        // Simple round-robin allocation without grouping by file
        for (size_t i = 0; i < sortedIndices.size(); ++i) {
          threadAssignments[currentThread].push_back(sortedIndices[i]);
          currentThread = (currentThread + 1) % numThreads;
        }
      }

      // Create threads with their assigned operations
      std::vector<std::thread> threads;
      for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back(
            &S3ReadBenchmark::workerThread,
            this,
            std::cref(operations),
            std::cref(threadAssignments[i]),
            i);
      }

      // Wait for all threads to complete
      for (auto& thread : threads) {
        thread.join();
      }

      auto endTime = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
          endTime - startTime);

      // Calculate statistics
      int64_t totalBytes = 0;
      for (const auto& op : operations) {
        totalBytes += op.size();
      }

      double throughputMBps =
          (totalBytes / (1024.0 * 1024.0)) / (duration.count() / 1000.0);

      LOG(INFO) << "Iteration " << (iter + 1) << " completed in "
                << duration.count() << " ms";
      LOG(INFO) << "Total bytes read: " << totalBytes << " ("
                << (totalBytes / (1024.0 * 1024.0)) << " MB)";
      LOG(INFO) << "Throughput: " << throughputMBps << " MB/s";
      LOG(INFO) << "Operations per second: "
                << (operations.size() * 1000.0 / duration.count());
    }
  }

  // Print summary of operations
  void printSummary(const std::vector<S3ReadOperation>& operations) {
    if (operations.empty()) {
      LOG(INFO) << "No operations loaded";
      return;
    }

    // Group operations by file
    std::map<std::string, std::vector<const S3ReadOperation*>> fileOps;
    int64_t totalBytes = 0;

    for (const auto& op : operations) {
      fileOps[op.file].push_back(&op);
      totalBytes += op.size();
    }

    LOG(INFO) << "=== Trace Log Summary ===";
    LOG(INFO) << "Total operations: " << operations.size();
    LOG(INFO) << "Unique files: " << fileOps.size();
    LOG(INFO) << "Total bytes to read: " << totalBytes << " ("
              << (totalBytes / (1024.0 * 1024.0)) << " MB)";

    LOG(INFO) << "\nOperations per file:";
    for (const auto& [file, ops] : fileOps) {
      int64_t fileBytes = 0;
      for (const auto* op : ops) {
        fileBytes += op->size();
      }
      LOG(INFO) << "  " << file << ": " << ops.size() << " operations, "
                << (fileBytes / (1024.0 * 1024.0)) << " MB";
    }
  }

  // Load S3 configuration from dump file
  std::unordered_map<std::string, std::string> loadConfigFromDump(const std::string& dumpPath) {
    std::unordered_map<std::string, std::string> configValues;
    std::ifstream dumpFile(dumpPath);
    
    if (!dumpFile.is_open()) {
      LOG(WARNING) << "Could not open S3 config dump file: " << dumpPath;
      LOG(INFO) << "Using default S3 configuration";
      return configValues;
    }

    LOG(INFO) << "Loading S3 configuration from: " << dumpPath;
    
    std::string line;
    std::string currentBucket;
    bool inConfigBlock = false;
    
    while (std::getline(dumpFile, line)) {
      // Check for start of config block
      if (line.find("=== S3Config dump at") != std::string::npos) {
        inConfigBlock = true;
        continue;
      }
      
      // Check for end of config block
      if (line.find("===") != std::string::npos && inConfigBlock) {
        break; // Use only the first (most recent) config block
      }
      
      if (!inConfigBlock || line.empty()) {
        continue;
      }
      
      // Parse key-value pairs
      size_t colonPos = line.find(':');
      if (colonPos != std::string::npos) {
        std::string key = line.substr(0, colonPos);
        std::string value = line.substr(colonPos + 1);
        
        // Trim whitespace
        key.erase(0, key.find_first_not_of(" \t"));
        key.erase(key.find_last_not_of(" \t") + 1);
        value.erase(0, value.find_first_not_of(" \t"));
        value.erase(value.find_last_not_of(" \t") + 1);
        
        // Skip "not set" values
        if (value == "not set") {
          continue;
        }
        
        // Map dump keys to config keys
        if (key == "bucket") {
          currentBucket = value;
          LOG(INFO) << "  Loaded bucket: " << value;
        } else if (key == "endpoint") {
          configValues["hive.s3.endpoint"] = value;
          LOG(INFO) << "  Loaded endpoint: " << value;
        } else if (key == "endpointRegion") {
          configValues["hive.s3.endpoint.region"] = value;
          LOG(INFO) << "  Loaded endpointRegion: " << value;
        } else if (key == "accessKey") {
          configValues["hive.s3.aws-access-key"] = value;
          LOG(INFO) << "  Loaded accessKey: " << value;
        } else if (key == "secretKey" && value != "***REDACTED***") {
          configValues["hive.s3.aws-secret-key"] = value;
          LOG(INFO) << "  Loaded secretKey: [REDACTED]";
        } else if (key == "useVirtualAddressing") {
          // Invert for path-style-access
          configValues["hive.s3.path-style-access"] = (value == "1" || value == "true") ? "false" : "true";
          LOG(INFO) << "  Loaded path-style-access: " << configValues["hive.s3.path-style-access"];
        } else if (key == "useSSL") {
          configValues["hive.s3.ssl.enabled"] = value;
          LOG(INFO) << "  Loaded ssl.enabled: " << value;
        } else if (key == "useInstanceCredentials") {
          configValues["hive.s3.use-instance-credentials"] = value;
          LOG(INFO) << "  Loaded use-instance-credentials: " << value;
        } else if (key == "iamRole") {
          configValues["hive.s3.iam-role"] = value;
          LOG(INFO) << "  Loaded iam-role: " << value;
        } else if (key == "iamRoleSessionName") {
          configValues["hive.s3.iam-role-session-name"] = value;
          LOG(INFO) << "  Loaded iam-role-session-name: " << value;
        } else if (key == "connectTimeout") {
          configValues["hive.s3.connect-timeout"] = value;
          LOG(INFO) << "  Loaded connect-timeout: " << value;
        } else if (key == "socketTimeout") {
          configValues["hive.s3.socket-timeout"] = value;
          LOG(INFO) << "  Loaded socket-timeout: " << value;
        } else if (key == "maxConnections") {
          configValues["hive.s3.max-connections"] = value;
          LOG(INFO) << "  Loaded max-connections: " << value;
        } else if (key == "maxAttempts") {
          configValues["hive.s3.max-attempts"] = value;
          LOG(INFO) << "  Loaded max-attempts: " << value;
        } else if (key == "retryMode") {
          configValues["hive.s3.retry-mode"] = value;
          LOG(INFO) << "  Loaded retry-mode: " << value;
        } else if (key == "useProxyFromEnv") {
          configValues["hive.s3.use-proxy-from-env"] = value;
          LOG(INFO) << "  Loaded use-proxy-from-env: " << value;
        } else if (key == "credentialsProvider") {
          configValues["hive.s3.aws-credentials-provider"] = value;
          LOG(INFO) << "  Loaded aws-credentials-provider: " << value;
        } else if (key == "useIMDS") {
          configValues["hive.s3.aws-imds-enabled"] = value;
          LOG(INFO) << "  Loaded aws-imds-enabled: " << value;
        } else if (key == "minPartSize") {
          // Convert bytes to MB format (e.g., "10485760" -> "10MB")
          try {
            size_t bytes = std::stoull(value);
            size_t mb = bytes / (1024 * 1024);
            configValues["hive.s3.min-part-size"] = std::to_string(mb) + "MB";
            LOG(INFO) << "  Loaded min-part-size: " << configValues["hive.s3.min-part-size"] << " (" << value << " bytes)";
          } catch (const std::exception& e) {
            LOG(WARNING) << "  Failed to parse minPartSize: " << value;
          }
        }
      }
    }
    
    dumpFile.close();
    
    if (configValues.empty()) {
      LOG(WARNING) << "No configuration loaded from dump file";
    } else {
      LOG(INFO) << "Successfully loaded " << configValues.size() << " configuration values";
    }
    
    return configValues;
  }

 private:
  std::shared_ptr<const config::ConfigBase> config_;
};

} // namespace facebook::velox::filesystems

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, true};

  if (FLAGS_trace_log_file.empty()) {
    LOG(ERROR) << "Please provide --trace_log_file parameter";
    LOG(INFO) << "Usage: " << argv[0]
              << " --trace_log_file=<path> [--num_threads=4] "
                 "[--iterations=1] [--verbose=false] "
                 "[--sort_by_file=true] [--group_same_file=true]";
    LOG(INFO) << "\nExample trace log format:";
    LOG(INFO) << "GetObject timestamp=1778968760063 "
                 "bucket=adobe-workload-east-1 "
                 "key=xdm/62b89f2ae63a221b63b2f6c1/_ACP_DATE=2020-01-29/"
                 "_ACP_BATCHID=01G6GN2E24FARAA1A456Y3W75D/"
                 "part-01022-17ef7759-0c4e-409a-a9ee-28ff48397539.c000.snappy."
                 "parquet range=bytes=86250071-86377805";
    return 1;
  }

  try {
    facebook::velox::filesystems::S3ReadBenchmark benchmark;

    // Load trace log
    auto operations = benchmark.loadTraceLog(FLAGS_trace_log_file);

    // Print summary
    benchmark.printSummary(operations);

    // Run benchmark
    benchmark.runBenchmark(
        operations, FLAGS_num_threads, FLAGS_iterations);

    LOG(INFO) << "Benchmark completed successfully";
  } catch (const std::exception& e) {
    LOG(ERROR) << "Benchmark failed: " << e.what();
    return 1;
  }

  return 0;
}

// Made with Bob
