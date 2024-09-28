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

#include "velox/exec/RowsStreamingWindowBuild.h"
#include "velox/common/testutil/TestValue.h"
#include <iostream>
#include <jemalloc/jemalloc.h>

extern uint64_t researved_size;

namespace facebook::velox::exec {

RowsStreamingWindowBuild::RowsStreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection)
    : WindowBuild(windowNode, pool, spillConfig, nonReclaimableSection),
    inputRows_(0, memory::StlAllocator<char*>(*pool)){
  velox::common::testutil::TestValue::adjust(
      "facebook::velox::exec::RowsStreamingWindowBuild::RowsStreamingWindowBuild",
      this);
  // inversedInputChannels_ and sortKeyInfo_ are initialized in the base class.
  // setup the fist windowpartition
  windowPartitions_.emplace_back(std::make_shared<WindowPartition>(pool_, data_.get(), inversedInputChannels_, sortKeyInfo_));
}

void RowsStreamingWindowBuild::addPartitionInputs(bool finished) {
  if (inputRows_.empty()) {
    return;
  }

  windowPartitions_.back()->addRows(inputRows_);

  if (finished) {
    windowPartitions_.back()->setComplete();
    ++inputPartition_;
    // Create a new partition for the next input.
    windowPartitions_.emplace_back(std::make_shared<WindowPartition>(pool_, data_.get(), inversedInputChannels_, sortKeyInfo_));
  }

  inputRows_.clear();
  inputRows_.shrink_to_fit();
}
bool RowsStreamingWindowBuild::needsInput() {
  // No partitions are available or the currentPartition is the last available
  // one, so can consume input rows.
  return windowPartitions_.empty() ||
      outputPartition_ == inputPartition_;
}

void RowsStreamingWindowBuild::addInput(RowVectorPtr input) {

  for (auto i = 0; i < inputChannels_.size(); ++i) {
    decodedInputVectors_[i].decode(*input->childAt(inputChannels_[i]));
  }

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }

    if (previousRow_ != nullptr &&
        compareRowsWithKeys(previousRow_, newRow, partitionKeyInfo_)) {
      addPartitionInputs(true);
    }

    if (previousRow_ != nullptr && inputRows_.size() >= numRowsPerOutput_) {
      addPartitionInputs(false);
    }

    inputRows_.push_back(newRow);
    previousRow_ = newRow;
  }
}

void RowsStreamingWindowBuild::noMoreInput() {
  //je_gluten_malloc_stats_print(NULL, NULL, NULL);
  addPartitionInputs(true);
}

std::shared_ptr<WindowPartition> RowsStreamingWindowBuild::nextPartition() {
  VELOX_CHECK(hasNextPartition());
  outputPartition_++;
  std::shared_ptr<WindowPartition> output = std::move(windowPartitions_.front());
  windowPartitions_.pop_front();
  return output;
}

bool RowsStreamingWindowBuild::hasNextPartition() {
  return !windowPartitions_.empty() &&
      outputPartition_ + 1 <= inputPartition_;
}

} // namespace facebook::velox::exec
