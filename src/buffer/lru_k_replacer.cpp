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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), current_timestamp_(std::time(nullptr) * 1000) {
  nodes_ = new LRUKNode[num_frames];
  for (size_t i = 0; i < num_frames; i++) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

LRUKReplacer::~LRUKReplacer() { delete[] nodes_; }

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> latch(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  frame_id_t evict_frame_id;
  if (!less_k_queue_.empty()) {
    size_t earliest_timestamp = ULONG_MAX;
    for (auto &entry : less_k_queue_) {
      if (entry.second < earliest_timestamp) {
        evict_frame_id = entry.first;
        earliest_timestamp = entry.second;
      }
    }
  } else {
    size_t max_distance = 0;
    for (auto &entry : k_queue_) {
      auto distance = current_timestamp_ - entry.second;
      if (distance > max_distance) {
        evict_frame_id = entry.first;
        max_distance = distance;
      }
    }
  }
  *frame_id = evict_frame_id;
  RemoveNode(evict_frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::scoped_lock<std::mutex> latch(latch_);
  size_t current_time = current_timestamp_++;
  if (node_store_.count(frame_id) == 0) {
    if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
      throw Exception(ExceptionType::INVALID, "LRU-K REPLACER: Invalid Frame Request");
    }
    auto new_node_id = free_list_.front();
    free_list_.pop_front();
    LRUKNode &node = nodes_[new_node_id];
    node.k_++;
    node.history_.push_front(current_time);
    node.frame_id_ = frame_id;
    node_store_[frame_id] = new_node_id;
  } else {
    auto &node = nodes_[node_store_.at(frame_id)];
    size_t k = node.k_ + 1;
    if (k <= k_) {
      node.k_++;
      node.history_.push_front(current_time);
    } else {
      node.history_.pop_back();
      node.history_.push_front(current_time);
    }
    if (node.is_evictable_) {
      EnQueue(frame_id, node);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> latch(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  if (frame_id < 0) {
    throw Exception(ExceptionType::INVALID, "LRU-K REPLACER: the given frame_id is a invalid id");
  }
  auto &node = nodes_[node_store_.at(frame_id)];
  if ((set_evictable ^ node.is_evictable_) == 0) {
    return;
  }
  node.is_evictable_ = set_evictable;
  if (!set_evictable) {
    DeQueue(frame_id);
  } else {
    EnQueue(frame_id, node);
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> latch(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  if (frame_id < 0) {
    throw Exception(ExceptionType::INVALID, "LRU-K REPLACER: the given frame_id is a invalid id");
  }
  RemoveNode(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> latch(latch_);
  return curr_size_;
}

void LRUKReplacer::EnQueue(frame_id_t frame_id, const LRUKNode &node) {
  size_t k = node.k_;
  if (k < k_) {
    less_k_queue_[frame_id] = node.history_.back();
  } else {
    if (less_k_queue_.count(frame_id) == 1) {
      less_k_queue_.erase(frame_id);
    }
    k_queue_[frame_id] = node.history_.back();
  }
}

void LRUKReplacer::DeQueue(frame_id_t frame_id) {
  less_k_queue_.erase(frame_id);
  k_queue_.erase(frame_id);
  curr_size_--;
}

void LRUKReplacer::RemoveNode(frame_id_t frame_id) {
  auto node_id = node_store_.at(frame_id);
  auto &node = nodes_[node_id];
  if (node.is_evictable_) {
    node.history_.clear();
    node.k_ = 0;
    node.frame_id_ = -1;
    node.is_evictable_ = false;
    DeQueue(frame_id);
    free_list_.push_back(node_id);
    node_store_.erase(frame_id);
  } else {
    throw Exception(ExceptionType::INVALID, "LRU-K REPLACER: the removed frame is non-evictable");
  }
}

}  // namespace bustub
