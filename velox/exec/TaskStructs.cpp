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

#include "velox/exec/TaskStructs.h"
#include <glog/logging.h>

namespace facebook::velox::exec {

namespace {

// Helper function to get the number of column chunks loaded in memory
// for a preloaded split.
int getColumnChunksLoaded(
    const std::shared_ptr<connector::ConnectorSplit>& connectorSplit) {
  if (!connectorSplit->dataSource || !connectorSplit->dataSource->hasValue()) {
    return 0;
  }

  // Check the atomic counter that gets incremented when each column chunk I/O completes.
  // Returns 0 if no data has been loaded yet.
  return connectorSplit->firstRowGroupBuffered.load(std::memory_order_acquire);
}

} // namespace

void SplitsStore::addSplit(
    Split split,
    std::vector<ContinuePromise>& promises) {
  VELOX_CHECK(!noMoreSplits_);
  VELOX_CHECK(!(remoteSplit_ && split.isBarrier()));
  VELOX_CHECK(barrierSplits_.empty());
  if (split.isBarrier()) {
    for (auto i = 0; i < split.barrier->numDrivers; ++i) {
      barrierSplits_[i] = Split::createBarrier();
    }
    VELOX_CHECK_LE(promises_.size(), split.barrier->numDrivers);
    // A barrier is assigned to every driver; wake up all currently blocked
    // drivers to process it.
    std::move(promises_.begin(), promises_.end(), std::back_inserter(promises));
    promises_.clear();
  } else {
    splits_.push_back(std::move(split));
    if (!promises_.empty()) {
      promises.push_back(std::move(promises_.back()));
      promises_.pop_back();
    }
  }
}

ContinueFuture SplitsStore::makeFuture() {
  auto [promise, future] =
      makeVeloxContinuePromiseContract("SplitsStore::makeFuture");
  promises_.push_back(std::move(promise));
  return std::move(future);
}

Split SplitsStore::getSplit(
    int maxPreloadSplits,
    const ConnectorSplitPreloadFunc& preload) {
  int readySplitIndex = -1;
  int maxColumnChunksLoaded = 0;
  int firstDataSourceReadyIndex = -1;
  if (maxPreloadSplits > 0) {
    for (int i = 0, end = std::min<size_t>(maxPreloadSplits, splits_.size());
         i < end;
         ++i) {
      if (splits_[i].isBarrier()) {
        VELOX_CHECK(!remoteSplit_);
        continue;
      }
      auto& connectorSplit = splits_[i].connectorSplit;
      if (!connectorSplit->dataSource) {
        // Initializes split->dataSource.
        preload(connectorSplit);
        preloadingSplits_->insert(connectorSplit);
      } else {
        // Track the first split with dataSource ready
        if (firstDataSourceReadyIndex == -1 && connectorSplit->dataSource->hasValue()) {
          firstDataSourceReadyIndex = i;
        }
        // Check how many column chunks have been loaded for this split
        int chunksLoaded = getColumnChunksLoaded(connectorSplit);
        if (chunksLoaded > maxColumnChunksLoaded) {
          // Prioritize splits with the most column chunks already loaded in memory
          maxColumnChunksLoaded = chunksLoaded;
          readySplitIndex = i;
          preloadingSplits_->erase(connectorSplit);
        }
      }
    }
  }
  // Selection priority:
  // 1. Split with maximum column chunks loaded (if any have chunks loaded)
  // 2. First split with dataSource ready (if no chunks loaded yet)
  // 3. First split (splits[0])
  if (readySplitIndex == -1) {
    if (firstDataSourceReadyIndex != -1) {
      readySplitIndex = firstDataSourceReadyIndex;
    } else {
      readySplitIndex = 0;
    }
  } else {
      LOG(INFO) << "Selected split index: " << readySplitIndex
            << ", column chunks loaded: " << chunksLoaded;
  }

  VELOX_CHECK(!splits_.empty());
  auto split = std::move(splits_[readySplitIndex]);
  splits_.erase(splits_.begin() + readySplitIndex);
  --taskStats_->numQueuedSplits;
  ++taskStats_->numRunningSplits;
  if (!remoteSplit_ && split.connectorSplit) {
    --taskStats_->numQueuedTableScanSplits;
    ++taskStats_->numRunningTableScanSplits;
    taskStats_->queuedTableScanSplitWeights -=
        split.connectorSplit->splitWeight;
    taskStats_->runningTableScanSplitWeights +=
        split.connectorSplit->splitWeight;
  }
  taskStats_->lastSplitStartTimeMs = getCurrentTimeMs();
  if (taskStats_->firstSplitStartTimeMs == 0) {
    taskStats_->firstSplitStartTimeMs = taskStats_->lastSplitStartTimeMs;
  }
  return split;
}

bool SplitsStore::tryGetBarrier(
    std::optional<uint32_t> driverId,
    Split& split) {
  if (!driverId.has_value()) {
    barrierSplits_.clear();
    return false;
  }
  // Delivers a barrier exactly once for each driver from the same plan node.
  auto it = barrierSplits_.find(*driverId);
  if (it == barrierSplits_.end()) {
    return false;
  }
  split = it->second;
  barrierSplits_.erase(it);
  return true;
}

} // namespace facebook::velox::exec
