//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <memory>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  queue_ = std::make_shared<LRUKQueue>();
  node_store_.reserve(replacer_size_);
  evictable_map_ = new bool[replacer_size_];
  for (size_t fid = 0; fid < replacer_size_; fid++) {
    evictable_map_[fid] = false;
  }
}

LRUKReplacer::~LRUKReplacer() { delete[] evictable_map_; }

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (curr_size_ == 0) {
    return false;
  }
  std::unique_lock<std::mutex> replacer_latch(latch_);
  std::unique_lock<std::mutex> queue_latch(queue_->latch_);
  auto &less_k_queue = queue_->less_k_queue_;
  auto &k_queue = queue_->k_queue_;
  if (!less_k_queue.empty()) {
    auto iter = less_k_queue.rbegin();
    auto end = less_k_queue.rend();
    for (; iter != end; iter++) {
      auto curr_id = *iter;
      if (evictable_map_[curr_id]) {
        *frame_id = curr_id;
        queue_->less_k_queue_.erase(queue_->on_less_k_queue_.at(*frame_id));
        queue_->on_less_k_queue_.erase(*frame_id);
        node_store_.erase(*frame_id);
        evictable_map_[*frame_id] = false;
        curr_size_--;
        return true;
      }
    }
  }
  size_t max_distance = 0;
  auto iter = k_queue.begin();
  auto end = k_queue.end();
  for (; iter != end; iter++) {
    auto curr = *iter;
    auto curr_fid = curr.first;
    if (!evictable_map_[curr_fid]) {
      continue;
    }
    auto distance = current_timestamp_ - curr.second;
    if (distance > max_distance) {
      *frame_id = curr_fid;
      max_distance = distance;
    }
  }
  queue_->k_queue_.erase(queue_->on_k_queue_.at(*frame_id));
  queue_->on_k_queue_.erase(*frame_id);
  node_store_.erase(*frame_id);
  evictable_map_[*frame_id] = false;
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_),
                "LRU-K REPLACER: Invalid frame request");
  std::unique_lock<std::mutex> replacer_latch(latch_);
  size_t timestamp = current_timestamp_++;
  std::shared_ptr<LRUKNode> node;
  if (node_store_.count(frame_id) == 0) {
    node = std::make_shared<LRUKNode>();
    node_store_[frame_id] = node;
  } else {
    node = node_store_.at(frame_id);
  }
  std::unique_lock<std::mutex> node_latch(node->latch_);
  replacer_latch.unlock();
  node->fid_ = frame_id;
  node->k_++;
  node->history_.push_front(timestamp);
  if (node->k_ > k_) {
    node->history_.pop_back();
  }
  EnQueue(node);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_),
                "LRU-K REPLACER: Invalid frame request");
  std::unique_lock<std::mutex> replacer_latch(latch_);
  if (node_store_.count(frame_id) == 0 || set_evictable == evictable_map_[frame_id]) {
    return;
  }
  evictable_map_[frame_id] = set_evictable;
  set_evictable ? curr_size_++ : curr_size_--;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> replacer_latch(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  BUSTUB_ASSERT(evictable_map_[frame_id], "LRU-K REPLACER: Remove non-evictable frame");
  DeQueue(frame_id);
  node_store_.erase(frame_id);
  evictable_map_[frame_id] = false;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::EnQueue(std::shared_ptr<LRUKNode> &node) {
  std::unique_lock<std::mutex> queue_latch(queue_->latch_);
  size_t k = node->k_;
  frame_id_t fid = node->fid_;
  auto &less_k_queue = queue_->less_k_queue_;
  auto &k_queue = queue_->k_queue_;
  auto &on_less_k_queue = queue_->on_less_k_queue_;
  auto &on_k_queue = queue_->on_k_queue_;
  if (k < k_) {
    if (on_less_k_queue.count(fid) == 0) {
      less_k_queue.push_front(fid);
      on_less_k_queue.insert({fid, less_k_queue.begin()});
    }
  } else {
    if (on_less_k_queue.count(fid) != 0) {
      less_k_queue.erase(on_less_k_queue.at(fid));
      on_less_k_queue.erase(fid);
    }
    if (on_k_queue.count(fid) != 0) {
      k_queue.erase(on_k_queue.at(fid));
      on_k_queue.erase(fid);
    }
    k_queue.push_front({fid, node->history_.back()});
    on_k_queue.insert({fid, k_queue.begin()});
  }
}

void LRUKReplacer::DeQueue(frame_id_t frame_id) {
  std::unique_lock<std::mutex> queue_latch(queue_->latch_);
  auto &on_less_k_queue = queue_->on_less_k_queue_;
  auto &on_k_queue = queue_->on_k_queue_;
  if (on_less_k_queue.count(frame_id) != 0) {
    queue_->less_k_queue_.erase(on_less_k_queue.at(frame_id));
    on_less_k_queue.erase(frame_id);
  }
  if (on_k_queue.count(frame_id) != 0) {
    queue_->k_queue_.erase(on_k_queue.at(frame_id));
    on_k_queue.erase(frame_id);
  }
}

}  // namespace bustub
