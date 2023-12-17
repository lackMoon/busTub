//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "type/limits.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (CanTxnTakeLock(txn, lock_mode, false)) {
    auto is_locked = IsHoldLock(txn, oid);
    if (is_locked.first) {
      auto curr_lock_mode = is_locked.second;
      return curr_lock_mode == lock_mode ? true : UpgradeLockTable(txn, curr_lock_mode, lock_mode, oid);
    }
    std::shared_ptr<LockRequestQueue> lock_queue;
    auto table_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    std::unique_lock<std::mutex> table_map_lck(table_lock_map_latch_);
    if (table_lock_map_.count(oid) == 0) {
      lock_queue = std::make_shared<LockRequestQueue>();
      table_lock_map_[oid] = lock_queue;
    } else {
      lock_queue = table_lock_map_.at(oid);
    }
    std::unique_lock<std::mutex> queue_lck(lock_queue->latch_);
    table_map_lck.unlock();
    auto &request_queue = lock_queue->request_queue_;
    if (CanLockGranted(request_queue, lock_mode)) {
      request_queue.push_back(table_request);
      table_request->granted_ = true;
      UpdateTransactionSet(txn, lock_mode, oid, true);
      return true;
    }
    request_queue.push_back(table_request);
    while (!table_request->granted_) {
      lock_queue->cv_.wait(queue_lck);
      if (txn->GetState() == TransactionState::ABORTED) {
        HandleAbort(txn, lock_queue.get(), table_request);
        return false;
      }
    }
    UpdateTransactionSet(txn, lock_mode, oid, true);
    return true;
  }
  return false;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  auto is_locked = IsHoldLock(txn, oid);
  if (is_locked.first) {
    if (!CheckNoLockRowsOnTable(txn, oid)) {
      AbortTxn(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
      return false;
    }
    auto txn_id = txn->GetTransactionId();
    std::unique_lock<std::mutex> table_map_lck(table_lock_map_latch_);
    std::shared_ptr<LockRequestQueue> lock_queue = table_lock_map_.at(oid);
    std::unique_lock<std::mutex> queue_lck(lock_queue->latch_);
    table_map_lck.unlock();
    auto &request_queue = lock_queue->request_queue_;
    std::shared_ptr<LockRequest> deleted_request;
    for (auto &request : request_queue) {
      if (request->txn_id_ == txn_id) {
        deleted_request = request;
        break;
      }
    }
    auto lock_mode = deleted_request->lock_mode_;
    request_queue.remove(deleted_request);
    GrantNewLocksIfPossible(lock_queue.get());
    UpdateTransactionSet(txn, lock_mode, oid, false);
    UpdateTransactionState(txn, lock_mode);
    return true;
  }
  AbortTxn(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (CanTxnTakeLock(txn, lock_mode, true)) {
    if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
      AbortTxn(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
    auto is_locked = IsHoldLock(txn, oid, rid);
    if (is_locked.first) {
      auto curr_lock_mode = is_locked.second;
      return curr_lock_mode == lock_mode ? true : UpgradeLockRow(txn, curr_lock_mode, lock_mode, oid, rid);
    }
    std::shared_ptr<LockRequestQueue> lock_queue;
    auto row_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    std::unique_lock<std::mutex> row_map_lck(row_lock_map_latch_);
    if (row_lock_map_.count(rid) == 0) {
      lock_queue = std::make_shared<LockRequestQueue>();
      row_lock_map_[rid] = lock_queue;
    } else {
      lock_queue = row_lock_map_.at(rid);
    }
    std::unique_lock<std::mutex> queue_lck(lock_queue->latch_);
    row_map_lck.unlock();
    auto &request_queue = lock_queue->request_queue_;
    if (CanLockGranted(request_queue, lock_mode)) {
      request_queue.push_back(row_request);
      row_request->granted_ = true;
      UpdateTransactionSet(txn, lock_mode, oid, rid, true);
      return true;
    }
    request_queue.push_back(row_request);
    while (!row_request->granted_) {
      lock_queue->cv_.wait(queue_lck);
      if (txn->GetState() == TransactionState::ABORTED) {
        HandleAbort(txn, lock_queue.get(), row_request);
        return false;
      }
    }
    UpdateTransactionSet(txn, lock_mode, oid, rid, true);
    return true;
  }
  return false;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  auto is_locked = IsHoldLock(txn, oid, rid);
  if (is_locked.first) {
    auto txn_id = txn->GetTransactionId();
    std::unique_lock<std::mutex> row_map_lck(row_lock_map_latch_);
    auto &lock_queue = row_lock_map_.at(rid);
    std::unique_lock<std::mutex> queue_lck(lock_queue->latch_);
    row_map_lck.unlock();
    auto &request_queue = lock_queue->request_queue_;
    std::shared_ptr<LockRequest> deleted_request;
    for (auto &request : request_queue) {
      if (request->txn_id_ == txn_id) {
        deleted_request = request;
        break;
      }
    }
    auto lock_mode = deleted_request->lock_mode_;
    request_queue.remove(deleted_request);
    GrantNewLocksIfPossible(lock_queue.get());
    UpdateTransactionSet(txn, lock_mode, oid, rid, false);
    if (!force) {
      UpdateTransactionState(txn, lock_mode);
    }
    return true;
  }
  AbortTxn(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  std::unique_lock<std::mutex> table_map_lck(table_lock_map_latch_);
  std::unique_lock<std::mutex> row_map_lck(row_lock_map_latch_);
  for (auto &row_requests : row_lock_map_) {
    auto &request_queue = row_requests.second;
    std::unique_lock<std::mutex> queue_lck(request_queue->latch_);
    request_queue->request_queue_.clear();
    request_queue->upgrading_ = INVALID_TXN_ID;
    queue_lck.unlock();
  }
  for (auto &tbl_requests : table_lock_map_) {
    auto &request_queue = tbl_requests.second;
    std::unique_lock<std::mutex> queue_lck(request_queue->latch_);
    request_queue->request_queue_.clear();
    request_queue->upgrading_ = INVALID_TXN_ID;
    queue_lck.unlock();
  }
  row_map_lck.unlock();
  table_map_lck.unlock();
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_graph_.count(t1) == 0) {
    waits_graph_.insert({t1, std::set<txn_id_t>()});
  }
  if (waits_graph_.count(t2) == 0) {
    waits_graph_.insert({t2, std::set<txn_id_t>()});
  }
  waits_graph_.at(t1).emplace(t2);
}

void LockManager::AddEdges(txn_id_t source_txn, std::vector<txn_id_t> &granted_set) {
  for (auto txn_id : granted_set) {
    waits_graph_.at(source_txn).emplace(txn_id);
  }
}
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &wait_set = waits_graph_.at(t1);
  if (wait_set.count(t2) != 0) {
    wait_set.erase(t2);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::vector<txn_id_t> path;
  std::unordered_set<txn_id_t> on_path;
  std::unordered_set<txn_id_t> visited;
  txn_id_t start;
  while (GetStarterNode(&start)) {
    if (FindCycle(start, path, on_path, visited, txn_id)) {
      return true;
    }
    search_txns_.insert(visited.begin(), visited.end());
  }
  return false;
}

auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  path.push_back(source_txn);
  on_path.insert(source_txn);
  while (!path.empty()) {
    auto curr_txn = path.back();
    auto &neighbors = waits_graph_.at(curr_txn);
    bool all_visited = true;
    for (auto &neighbor : neighbors) {
      if (on_path.count(neighbor) != 0) {
        auto iter = std::find(path.begin(), path.end(), neighbor);
        txn_id_t youngest_txn = -1;
        for (auto end = path.end(); iter != end; iter++) {
          auto node = *iter;
          if (node > youngest_txn) {
            youngest_txn = node;
          }
        }
        *abort_txn_id = youngest_txn;
        return true;
      }
      if (visited.count(neighbor) != 0 || abort_txns_.count(neighbor) != 0) {
        continue;
      }
      path.push_back(neighbor);
      on_path.insert(neighbor);
      all_visited = false;
      break;
    }
    visited.insert(curr_txn);
    if (all_visited) {
      path.pop_back();
      on_path.erase(curr_txn);
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto &txn_waits : waits_graph_) {
    auto source_txn_id = txn_waits.first;
    auto &wait_set = txn_waits.second;
    for (auto wait_txn_id : wait_set) {
      edges.emplace_back(source_txn_id, wait_txn_id);
    }
  }
  return edges;
}

auto LockManager::GetStarterNode(txn_id_t *start) -> bool {
  txn_id_t min_txn = BUSTUB_INT32_MAX;
  for (auto &node : waits_graph_) {
    auto txn_id = node.first;
    if (search_txns_.count(txn_id) != 0) {
      continue;
    }
    if (min_txn > txn_id) {
      min_txn = txn_id;
    }
  }
  if (min_txn != BUSTUB_INT32_MAX) {
    *start = min_txn;
    return true;
  }
  return false;
}

void LockManager::BuildGraph(LockRequestQueue *lock_request_queue) {
  std::unique_lock<std::mutex> queue_lck(lock_request_queue->latch_);
  auto &request_queue = lock_request_queue->request_queue_;
  std::vector<txn_id_t> granted_txns;
  for (auto &request : request_queue) {
    auto txn_id = request->txn_id_;
    if (abort_txns_.count(txn_id) != 0) {
      continue;
    }
    if (txn_manager_->GetTransaction(txn_id)->GetState() == TransactionState::ABORTED) {
      abort_txns_.insert(txn_id);
      continue;
    }
    if (waits_graph_.count(txn_id) == 0) {
      waits_graph_.insert({txn_id, std::set<txn_id_t>()});
    }
    if (request->granted_) {
      granted_txns.push_back(request->txn_id_);
    } else {
      waiting_txns_.insert({txn_id, waiting_txns_.at(-1)});
      AddEdges(txn_id, granted_txns);
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> table_map_lck(table_lock_map_latch_);
      std::unique_lock<std::mutex> row_map_lck(row_lock_map_latch_);
      for (auto &tbl_requests : table_lock_map_) {
        waiting_txns_[-1] = {true, tbl_requests.first};
        BuildGraph(tbl_requests.second.get());
      }
      for (auto &row_requests : row_lock_map_) {
        waiting_txns_[-1] = {false, row_requests.first.Get()};
        BuildGraph(row_requests.second.get());
      }
      waiting_txns_.erase(-1);
      row_map_lck.unlock();
      table_map_lck.unlock();
      txn_id_t abort_txn;
      while (HasCycle(&abort_txn)) {
        txn_manager_->GetTransaction(abort_txn)->SetState(TransactionState::ABORTED);
        waits_graph_.erase(abort_txn);
        abort_txns_.insert(abort_txn);
        auto id = waiting_txns_.at(abort_txn);
        auto &queue = id.first ? table_lock_map_.at(id.second) : row_lock_map_.at(RID(id.second));
        queue->cv_.notify_all();
      }
      search_txns_.clear();
      abort_txns_.clear();
      waiting_txns_.clear();
      waits_graph_.clear();
    }
  }
}

}  // namespace bustub
