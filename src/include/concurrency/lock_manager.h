//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "type/limits.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() = default;

  void StartDeadlockDetection() {
    BUSTUB_ENSURE(txn_manager_ != nullptr, "txn_manager_ is not set.")
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    UnlockAll();

    enable_cycle_detection_ = false;

    if (cycle_detection_thread_ != nullptr) {
      cycle_detection_thread_->join();
      delete cycle_detection_thread_;
    }
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *    - Basically there should be three steps to perform a lock upgrade in general
   *      - 1. Check the precondition of upgrade
   *      - 2. Drop the current lock, reserve the upgrade position
   *      - 3. Wait to get the new lock granted
   *
   *    A lock request being upgraded should be prioritized over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   *
   *    You probably want to consider which type of lock to directly apply on table
   *    when implementing executor later
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @param force unlock the tuple regardless of isolation level, not changing the transaction state
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force = false) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  TransactionManager *txn_manager_;

 private:
  /** Spring 2023 */
  /* You are allowed to modify all functions below. */
  auto UpgradeLockTable(Transaction *txn, LockMode curr_lock_mode, LockMode requested_lock_mode, const table_oid_t &oid)
      -> bool {
    if (CanLockUpgrade(curr_lock_mode, requested_lock_mode)) {
      auto upgrade_request = std::make_shared<LockRequest>(txn->GetTransactionId(), requested_lock_mode, oid);
      std::unique_lock<std::mutex> table_lck(table_lock_map_latch_);
      auto lock_queue = table_lock_map_.at(oid);
      if (lock_queue->upgrading_ != INVALID_TXN_ID) {
        AbortTxn(txn, AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      std::unique_lock<std::mutex> queue_lck(lock_queue->latch_);
      table_lck.unlock();
      auto txn_id = txn->GetTransactionId();
      auto &request_queue = lock_queue->request_queue_;
      std::shared_ptr<LockRequest> deleted_request;
      for (auto &request : request_queue) {
        if (request->txn_id_ == txn_id) {
          deleted_request = request;
          break;
        }
      }
      request_queue.remove(deleted_request);
      UpdateTransactionSet(txn, curr_lock_mode, oid, false);
      auto iter = request_queue.begin();
      auto end = request_queue.end();
      while (iter != end) {
        auto &request = *iter;
        if (!request->granted_) {
          break;
        }
        iter++;
      }
      if (iter != end) {
        request_queue.insert(iter, upgrade_request);
      } else {
        request_queue.push_back(upgrade_request);
      }
      lock_queue->upgrading_ = txn_id;
      GrantNewLocksIfPossible(lock_queue.get());
      while (!upgrade_request->granted_) {
        lock_queue->cv_.wait(queue_lck);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_queue->upgrading_ = INVALID_TXN_ID;
          HandleAbort(txn, lock_queue.get(), upgrade_request);
          return false;
        }
      }
      lock_queue->upgrading_ = INVALID_TXN_ID;
      UpdateTransactionSet(txn, requested_lock_mode, oid, true);
      return true;
    }
    AbortTxn(txn, AbortReason::INCOMPATIBLE_UPGRADE);
    return false;
  }
  auto UpgradeLockRow(Transaction *txn, LockMode curr_lock_mode, LockMode requested_lock_mode, const table_oid_t &oid,
                      const RID &rid) -> bool {
    if (CanLockUpgrade(curr_lock_mode, requested_lock_mode)) {
      auto upgrade_request = std::make_shared<LockRequest>(txn->GetTransactionId(), requested_lock_mode, oid, rid);
      std::unique_lock<std::mutex> row_lck(row_lock_map_latch_);
      auto lock_queue = row_lock_map_.at(rid);
      if (lock_queue->upgrading_ != INVALID_TXN_ID) {
        AbortTxn(txn, AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      std::unique_lock<std::mutex> queue_lck(lock_queue->latch_);
      row_lck.unlock();
      auto txn_id = txn->GetTransactionId();
      auto &request_queue = lock_queue->request_queue_;
      std::shared_ptr<LockRequest> deleted_request;
      for (auto &request : request_queue) {
        if (request->txn_id_ == txn_id) {
          deleted_request = request;
          break;
        }
      }
      request_queue.remove(deleted_request);
      UpdateTransactionSet(txn, curr_lock_mode, oid, rid, false);
      auto iter = request_queue.begin();
      auto end = request_queue.end();
      while (iter != end) {
        auto &request = *iter;
        if (!request->granted_) {
          break;
        }
        iter++;
      }
      if (iter != end) {
        request_queue.insert(iter, upgrade_request);
      } else {
        request_queue.push_back(upgrade_request);
      }
      lock_queue->upgrading_ = txn_id;
      GrantNewLocksIfPossible(lock_queue.get());
      while (!upgrade_request->granted_) {
        lock_queue->cv_.wait(queue_lck);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_queue->upgrading_ = INVALID_TXN_ID;
          HandleAbort(txn, lock_queue.get(), upgrade_request);
          return false;
        }
      }
      lock_queue->upgrading_ = INVALID_TXN_ID;
      UpdateTransactionSet(txn, requested_lock_mode, oid, rid, true);
      return true;
    }
    AbortTxn(txn, AbortReason::INCOMPATIBLE_UPGRADE);
    return false;
  }
  auto IsHoldLock(Transaction *txn, const table_oid_t &oid) -> std::pair<bool, LockMode> {
    if (txn->IsTableSharedLocked(oid)) {
      return {true, LockMode::SHARED};
    }
    if (txn->IsTableExclusiveLocked(oid)) {
      return {true, LockMode::EXCLUSIVE};
    }
    if (txn->IsTableIntentionSharedLocked(oid)) {
      return {true, LockMode::INTENTION_SHARED};
    }
    if (txn->IsTableIntentionExclusiveLocked(oid)) {
      return {true, LockMode::INTENTION_EXCLUSIVE};
    }
    if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return {true, LockMode::SHARED_INTENTION_EXCLUSIVE};
    }
    return {false, LockMode()};
  }

  auto IsHoldLock(Transaction *txn, const table_oid_t &oid, const RID &rid) -> std::pair<bool, LockMode> {
    if (txn->IsRowSharedLocked(oid, rid)) {
      return {true, LockMode::SHARED};
    }
    if (txn->IsRowExclusiveLocked(oid, rid)) {
      return {true, LockMode::EXCLUSIVE};
    }
    return {false, LockMode()};
  }
  auto AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
    switch (l1) {
      case LockMode::SHARED:
        return l2 == LockMode::SHARED || l2 == LockMode::INTENTION_SHARED;
      case LockMode::EXCLUSIVE:
        return false;
      case LockMode::INTENTION_SHARED:
        return l2 != LockMode::EXCLUSIVE;
      case LockMode::INTENTION_EXCLUSIVE:
        return l2 == LockMode::INTENTION_EXCLUSIVE || l2 == LockMode::INTENTION_SHARED;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return l2 == LockMode::INTENTION_SHARED;
    }
  }
  auto CanTxnTakeLock(Transaction *txn, LockMode lock_mode, bool is_row) -> bool {
    if (is_row) {
      if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        AbortTxn(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
        return false;
      }
    }
    auto state = txn->GetState();
    auto level = txn->GetIsolationLevel();
    switch (level) {
      case IsolationLevel::READ_UNCOMMITTED:
        if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
          AbortTxn(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
          return false;
        }
      case IsolationLevel::REPEATABLE_READ:
        if (state == TransactionState::SHRINKING) {
          AbortTxn(txn, AbortReason::LOCK_ON_SHRINKING);
          return false;
        }
        return true;
      case IsolationLevel::READ_COMMITTED:
        if (state == TransactionState::SHRINKING &&
            !(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) {
          AbortTxn(txn, AbortReason::LOCK_ON_SHRINKING);
          return false;
        }
        return true;
    }
  }
  auto CanLockGranted(std::list<std::shared_ptr<LockRequest>> &lock_request_queue, LockMode lock_mode) -> bool {
    if (lock_request_queue.empty()) {
      return true;
    }
    bool is_compatible = true;
    for (auto &request : lock_request_queue) {
      if (!(request->granted_ && AreLocksCompatible(lock_mode, request->lock_mode_))) {
        is_compatible = false;
        break;
      }
    }
    return is_compatible;
  }

  void GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {
    auto &request_queue = lock_request_queue->request_queue_;
    bool is_granted = false;
    if (request_queue.empty()) {
      return;
    }
    auto &head_request = request_queue.front();
    if (!head_request->granted_) {
      head_request->granted_ = true;
      is_granted = true;
    }
    auto iter = request_queue.begin();
    iter++;
    for (auto end = request_queue.end(); iter != end; iter++) {
      auto &curr_request = *iter;
      if (curr_request->granted_) {
        continue;
      }
      if (!AreLocksCompatible(curr_request->lock_mode_, head_request->lock_mode_)) {
        break;
      }
      curr_request->granted_ = true;
      is_granted = true;
    }
    if (is_granted) {
      lock_request_queue->cv_.notify_all();
    }
  }
  void UpdateTransactionSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, bool is_insert) {
    std::shared_ptr<std::unordered_set<table_oid_t>> set;
    switch (lock_mode) {
      case LockMode::SHARED:
        set = txn->GetSharedTableLockSet();
        break;
      case LockMode::EXCLUSIVE:
        set = txn->GetExclusiveTableLockSet();
        break;
      case LockMode::INTENTION_SHARED:
        set = txn->GetIntentionSharedTableLockSet();
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        set = txn->GetIntentionExclusiveTableLockSet();
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        set = txn->GetSharedIntentionExclusiveTableLockSet();
        break;
    }
    if (is_insert) {
      set->insert(oid);
    } else {
      set->erase(oid);
    }
  }

  void UpdateTransactionSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid,
                            bool is_insert) {
    auto set = lock_mode == LockMode::SHARED ? txn->GetSharedRowLockSet() : txn->GetExclusiveRowLockSet();
    if (set->count(oid) == 0) {
      set->insert({oid, std::unordered_set<RID>()});
    }
    if (is_insert) {
      set->at(oid).insert(rid);
    } else {
      set->at(oid).erase(rid);
    }
  }

  void UpdateTransactionState(Transaction *txn, LockMode lock_mode) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::READ_UNCOMMITTED:
          if (lock_mode == LockMode::SHARED) {
            throw Exception(ExceptionType::UNKNOWN_TYPE,
                            "undefined behaviour : unlocking an S lock under READ_UNCOMMITTED isolation level");
          }
        case IsolationLevel::READ_COMMITTED:
          if (lock_mode == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::REPEATABLE_READ:
          txn->SetState(TransactionState::SHRINKING);
          break;
      }
    }
  }

  auto CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
    switch (curr_lock_mode) {
      case LockMode::SHARED:
        return requested_lock_mode == LockMode::EXCLUSIVE ||
               requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
      case LockMode::EXCLUSIVE:
        return false;
      case LockMode::INTENTION_SHARED:
        return requested_lock_mode != LockMode::INTENTION_SHARED;
      case LockMode::INTENTION_EXCLUSIVE:
        return requested_lock_mode == LockMode::EXCLUSIVE ||
               requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        return requested_lock_mode == LockMode::EXCLUSIVE;
    }
  }
  auto CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool {
    return row_lock_mode == LockMode::SHARED ? true
                                             : txn->GetExclusiveTableLockSet()->count(oid) != 0 ||
                                                   txn->GetIntentionExclusiveTableLockSet()->count(oid) != 0 ||
                                                   txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) != 0;
  }

  auto CheckNoLockRowsOnTable(Transaction *txn, const table_oid_t &oid) -> bool {
    auto shared_locks = txn->GetSharedRowLockSet();
    auto exclusive_locks = txn->GetExclusiveRowLockSet();
    if (shared_locks->count(oid) == 0 && exclusive_locks->count(oid) == 0) {
      return true;
    }
    if (shared_locks->count(oid) != 0 && exclusive_locks->count(oid) != 0) {
      return shared_locks->at(oid).empty() && exclusive_locks->at(oid).empty();
    }
    return shared_locks->count(oid) == 0 ? exclusive_locks->at(oid).empty() : shared_locks->at(oid).empty();
  }
  void AbortTxn(Transaction *txn, AbortReason reason) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), reason);
  }

  void HandleAbort(Transaction *txn, LockRequestQueue *lock_request_queue, std::shared_ptr<LockRequest> &request) {
    lock_request_queue->request_queue_.remove(request);
    GrantNewLocksIfPossible(lock_request_queue);
  }

  auto AddEdges(txn_id_t source_txn, std::vector<txn_id_t> &granted_set) -> void;

  auto BuildGraph(LockRequestQueue *lock_request_queue) -> void;

  auto GetStarterNode(txn_id_t *start) -> bool;

  auto FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                 std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool;
  void UnlockAll();

  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;

  std::unordered_map<txn_id_t, std::set<txn_id_t>> waits_graph_;
  std::unordered_map<txn_id_t, std::pair<bool, int64_t>> waiting_txns_;
  std::unordered_set<txn_id_t> abort_txns_;
  std::set<txn_id_t> search_txns_;
};

}  // namespace bustub

template <>
struct fmt::formatter<bustub::LockManager::LockMode> : formatter<std::string_view> {
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(bustub::LockManager::LockMode x, FormatContext &ctx) const {
    string_view name = "unknown";
    switch (x) {
      case bustub::LockManager::LockMode::EXCLUSIVE:
        name = "EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::INTENTION_EXCLUSIVE:
        name = "INTENTION_EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::SHARED:
        name = "SHARED";
        break;
      case bustub::LockManager::LockMode::INTENTION_SHARED:
        name = "INTENTION_SHARED";
        break;
      case bustub::LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
        name = "SHARED_INTENTION_EXCLUSIVE";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
