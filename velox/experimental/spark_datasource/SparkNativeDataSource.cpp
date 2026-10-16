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

// A Spark native data source (apache/spark#59313) that reads Parquet files with Velox.
//
//   spark.read.format("velox").option("path", "/data/t").load()
//
// Options:
//   path        a Parquet file, or a directory of Parquet files (required)
//   batch_size  the number of rows of each Arrow batch, 4096 by default
//
// Batch reads only, without pushdown: each file is a partition, read with Velox's Parquet reader
// and exported to Spark with Velox's Arrow bridge. Only integer (tinyint, smallint, int, bigint)
// and string columns are supported.

#include <jni.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "velox/common/file/LocalFile.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/vector/arrow/Abi.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox;

namespace {

constexpr jint kAbiVersion = 1;
constexpr uint64_t kDefaultBatchSize = 4096;

// ---------------------------------------------------------------------------------------------
// Velox
// ---------------------------------------------------------------------------------------------

memory::MemoryPool* pool() {
  static std::once_flag once;
  static std::shared_ptr<memory::MemoryPool> leaf;
  std::call_once(once, [] {
    memory::MemoryManager::initialize(memory::MemoryManager::Options{});
    leaf = memory::memoryManager()->addLeafPool("spark_datasource_velox");
  });
  return leaf.get();
}

// The options of the Arrow export, as Spark reads them: no dictionary or constant encoding, which
// Spark rejects, and strings with 32-bit offsets (the default).
ArrowOptions arrowOptions() {
  ArrowOptions options;
  options.flattenDictionary = true;
  options.flattenConstant = true;
  return options;
}

void checkSupportedTypes(const RowTypePtr& rowType, const std::string& path) {
  for (size_t i = 0; i < rowType->size(); ++i) {
    switch (rowType->childAt(i)->kind()) {
      case TypeKind::TINYINT:
      case TypeKind::SMALLINT:
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
      case TypeKind::VARCHAR:
        break;
      default:
        throw std::runtime_error(
            "The column " + rowType->nameOf(i) + " of " + path + " has the type " +
            rowType->childAt(i)->toString() +
            ", but only integer and string columns are supported");
    }
  }
}

std::unique_ptr<parquet::ParquetReader> openFile(const std::string& path) {
  dwio::common::ReaderOptions options(pool());
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<LocalReadFile>(path), *pool());
  auto reader = std::make_unique<parquet::ParquetReader>(std::move(input), options);
  checkSupportedTypes(reader->rowType(), path);
  return reader;
}

std::unique_ptr<dwio::common::RowReader> createRowReader(
    const dwio::common::Reader& reader,
    const RowTypePtr& rowType) {
  dwio::common::RowReaderOptions options;
  options.select(
      std::make_shared<dwio::common::ColumnSelector>(rowType, rowType->names()));
  auto scanSpec = std::make_shared<common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  options.setScanSpec(scanSpec);
  return reader.createRowReader(options);
}

void exportSchema(const RowTypePtr& rowType, ArrowSchema* out) {
  exportToArrow(BaseVector::create(rowType, 0, pool()), *out, arrowOptions());
}

// The Parquet files of a path: the path itself, or the visible files of a directory.
std::vector<std::string> listFiles(const std::string& path) {
  namespace fs = std::filesystem;
  if (!fs::is_directory(path)) {
    return {path};
  }
  std::vector<std::string> files;
  for (const auto& entry : fs::directory_iterator(path)) {
    const auto name = entry.path().filename().string();
    if (entry.is_regular_file() && name[0] != '.' && name[0] != '_') {
      files.push_back(entry.path().string());
    }
  }
  std::sort(files.begin(), files.end());
  if (files.empty()) {
    throw std::runtime_error("No files found under " + path);
  }
  return files;
}

// ---------------------------------------------------------------------------------------------
// Handles
// ---------------------------------------------------------------------------------------------

struct DataSource {
  std::string path;
  uint64_t batchSize{kDefaultBatchSize};
};

struct Reader {
  std::vector<std::string> files;
  uint64_t batchSize;
};

// The partition of a file, sent to the executors: the batch size, a newline and the path.
std::string encodePartition(uint64_t batchSize, const std::string& file) {
  return std::to_string(batchSize) + "\n" + file;
}

std::pair<uint64_t, std::string> decodePartition(const std::string& partition) {
  const auto newline = partition.find('\n');
  if (newline == std::string::npos) {
    throw std::runtime_error("Invalid partition: " + partition);
  }
  return {std::stoull(partition.substr(0, newline)), partition.substr(newline + 1)};
}

// ---------------------------------------------------------------------------------------------
// ArrowArrayStream of a partition
// ---------------------------------------------------------------------------------------------

struct PartitionStream {
  std::unique_ptr<parquet::ParquetReader> reader;
  std::unique_ptr<dwio::common::RowReader> rowReader;
  RowTypePtr rowType;
  uint64_t batchSize;
  std::string lastError;
};

PartitionStream* streamOf(ArrowArrayStream* stream) {
  return static_cast<PartitionStream*>(stream->private_data);
}

int streamGetSchema(ArrowArrayStream* stream, ArrowSchema* out) {
  try {
    exportSchema(streamOf(stream)->rowType, out);
    return 0;
  } catch (const std::exception& e) {
    streamOf(stream)->lastError = e.what();
    return EIO;
  }
}

// Loads the lazy vectors that the Parquet reader may return, at the top level of the batch.
RowVectorPtr loaded(const VectorPtr& batch) {
  auto row = std::dynamic_pointer_cast<RowVector>(BaseVector::loadedVectorShared(batch));
  for (auto& child : row->children()) {
    child = BaseVector::loadedVectorShared(child);
  }
  return row;
}

int streamGetNext(ArrowArrayStream* stream, ArrowArray* out) {
  auto* state = streamOf(stream);
  try {
    VectorPtr batch = BaseVector::create(state->rowType, 0, pool());
    // `next` returns the number of rows scanned, 0 at the end of the file.
    while (state->rowReader->next(state->batchSize, batch) > 0) {
      if (batch->size() > 0) {
        exportToArrow(loaded(batch), *out, pool(), arrowOptions());
        return 0;
      }
    }
    out->release = nullptr; // The end of the stream.
    return 0;
  } catch (const std::exception& e) {
    state->lastError = e.what();
    return EIO;
  }
}

const char* streamGetLastError(ArrowArrayStream* stream) {
  const auto& error = streamOf(stream)->lastError;
  return error.empty() ? nullptr : error.c_str();
}

void streamRelease(ArrowArrayStream* stream) {
  delete streamOf(stream);
  stream->release = nullptr;
}

// ---------------------------------------------------------------------------------------------
// JNI helpers
// ---------------------------------------------------------------------------------------------

std::string toString(JNIEnv* env, jstring value) {
  const char* chars = env->GetStringUTFChars(value, nullptr);
  std::string result(chars);
  env->ReleaseStringUTFChars(value, chars);
  return result;
}

std::string toString(JNIEnv* env, jbyteArray value) {
  const auto length = env->GetArrayLength(value);
  std::string result(length, '\0');
  env->GetByteArrayRegion(value, 0, length, reinterpret_cast<jbyte*>(result.data()));
  return result;
}

// Runs `f`, and reports its exception as a Java exception: C++ exceptions must not cross JNI.
template <typename T, typename F>
T call(JNIEnv* env, T fallback, F&& f) {
  try {
    return f();
  } catch (const std::exception& e) {
    env->ThrowNew(env->FindClass("java/lang/RuntimeException"), e.what());
  } catch (...) {
    env->ThrowNew(env->FindClass("java/lang/RuntimeException"), "Unknown error in Velox");
  }
  return fallback;
}

} // namespace

// ---------------------------------------------------------------------------------------------
// org.apache.spark.sql.datasource.NativeBridge
// ---------------------------------------------------------------------------------------------

#define BRIDGE(ret, method) \
  extern "C" JNIEXPORT ret JNICALL Java_org_apache_spark_sql_datasource_NativeBridge_##method

BRIDGE(jint, abiVersion)(JNIEnv*, jclass) {
  return kAbiVersion;
}

BRIDGE(jlong, createDataSource)
(JNIEnv* env, jclass, jstring, jobjectArray keys, jobjectArray values) {
  return call(env, jlong{0}, [&] {
    std::map<std::string, std::string> options;
    for (jsize i = 0; i < env->GetArrayLength(keys); ++i) {
      options[toString(env, (jstring)env->GetObjectArrayElement(keys, i))] =
          toString(env, (jstring)env->GetObjectArrayElement(values, i));
    }
    auto source = std::make_unique<DataSource>();
    if (options.count("path") == 0) {
      throw std::runtime_error("The option 'path' is required");
    }
    source->path = options["path"];
    if (options.count("batch_size") > 0) {
      source->batchSize = std::stoull(options["batch_size"]);
    }
    return reinterpret_cast<jlong>(source.release());
  });
}

BRIDGE(void, closeDataSource)(JNIEnv*, jclass, jlong dataSource) {
  delete reinterpret_cast<DataSource*>(dataSource);
}

BRIDGE(void, schema)(JNIEnv* env, jclass, jlong dataSource, jlong schemaAddress) {
  call(env, 0, [&] {
    auto* source = reinterpret_cast<DataSource*>(dataSource);
    exportSchema(openFile(listFiles(source->path).front())->rowType(),
                 reinterpret_cast<ArrowSchema*>(schemaAddress));
    return 0;
  });
}

// The schema to read is ignored: without column pruning, the reader returns all the columns of
// the files, which is the schema returned by `schema`.
BRIDGE(jlong, createReader)(JNIEnv* env, jclass, jlong dataSource, jlong) {
  return call(env, jlong{0}, [&] {
    auto* source = reinterpret_cast<DataSource*>(dataSource);
    auto reader = std::make_unique<Reader>();
    reader->files = listFiles(source->path);
    reader->batchSize = source->batchSize;
    return reinterpret_cast<jlong>(reader.release());
  });
}

BRIDGE(jobjectArray, partitions)(JNIEnv* env, jclass, jlong readerHandle) {
  return call(env, (jobjectArray) nullptr, [&] {
    auto* reader = reinterpret_cast<Reader*>(readerHandle);
    auto result = env->NewObjectArray(reader->files.size(), env->FindClass("[B"), nullptr);
    for (size_t i = 0; i < reader->files.size(); ++i) {
      const auto partition = encodePartition(reader->batchSize, reader->files[i]);
      auto bytes = env->NewByteArray(partition.size());
      env->SetByteArrayRegion(
          bytes, 0, partition.size(), reinterpret_cast<const jbyte*>(partition.data()));
      env->SetObjectArrayElement(result, i, bytes);
      env->DeleteLocalRef(bytes);
    }
    return result;
  });
}

BRIDGE(void, closeReader)(JNIEnv*, jclass, jlong reader) {
  delete reinterpret_cast<Reader*>(reader);
}

BRIDGE(void, read)
(JNIEnv* env, jclass, jbyteArray, jbyteArray partition, jlong streamAddress) {
  call(env, 0, [&] {
    const auto [batchSize, file] = decodePartition(toString(env, partition));
    auto state = std::make_unique<PartitionStream>();
    state->reader = openFile(file);
    state->rowType = state->reader->rowType();
    state->rowReader = createRowReader(*state->reader, state->rowType);
    state->batchSize = batchSize;

    auto* stream = reinterpret_cast<ArrowArrayStream*>(streamAddress);
    stream->get_schema = streamGetSchema;
    stream->get_next = streamGetNext;
    stream->get_last_error = streamGetLastError;
    stream->release = streamRelease;
    stream->private_data = state.release();
    return 0;
  });
}
