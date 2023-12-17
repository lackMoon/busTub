//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
  using LockMode = LockManager::LockMode;

 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  auto IsNeedLock(table_oid_t oid) -> bool {
    if (txn_->IsTableExclusiveLocked(oid) || txn_->IsTableIntentionExclusiveLocked(oid)) {
      return false;
    }
    return is_deleted_ ? true : txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED;
  }

  auto IsNeedLock(table_oid_t oid, RID &rid) -> bool {
    if (txn_->IsTableExclusiveLocked(oid) || txn_->IsTableSharedLocked(oid) || txn_->IsRowExclusiveLocked(oid, rid)) {
      return false;
    }
    return is_deleted_ ? true : txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED;
  }

  void TryLockTable(table_oid_t oid) {
    if (IsNeedLock(oid)) {
      LockMode lock_mode = is_deleted_ ? LockMode::INTENTION_EXCLUSIVE : LockMode::INTENTION_SHARED;
      try {
        if (!lock_mgr_->LockTable(txn_, lock_mode, oid)) {
          throw ExecutionException(fmt::format("Failed to acquire the {} lock of {} table", lock_mode,
                                               exec_ctx_->GetCatalog()->GetTable(oid)->name_));
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException(fmt::format("Exception happened when acquire the {} lock of {} table : {}", lock_mode,
                                             exec_ctx_->GetCatalog()->GetTable(oid)->name_, e.GetInfo()));
      }
    }
  }

  void TryUnLockTable(table_oid_t oid) {
    if (IsNeedLock(oid) && (!is_deleted_ && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
      lock_mgr_->UnlockTable(txn_, oid);
    }
  }

  void TryLockTuple(table_oid_t oid, RID &rid) {
    if (IsNeedLock(oid, rid)) {
      LockMode lock_mode = is_deleted_ ? LockMode::EXCLUSIVE : LockMode::SHARED;
      try {
        if (!lock_mgr_->LockRow(txn_, lock_mode, oid, rid)) {
          throw ExecutionException(fmt::format("Failed to acquire the {} lock of tuple in {} table", lock_mode,
                                               exec_ctx_->GetCatalog()->GetTable(oid)->name_));
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException(fmt::format("Exception happened when acquire the {} lock of tuple in {} table : {}",
                                             lock_mode, exec_ctx_->GetCatalog()->GetTable(oid)->name_, e.GetInfo()));
      }
    }
  }

  void TryUnLockTuple(table_oid_t oid, RID &rid, bool force = false) {
    if (IsNeedLock(oid, rid)) {
      if (force || txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        lock_mgr_->UnlockRow(txn_, oid, rid, force);
      }
    }
  }
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;

  Schema schema_;

  std::unique_ptr<TableIterator> iterator_;

  Transaction *txn_;

  LockManager *lock_mgr_;

  bool is_deleted_;
};
}  // namespace bustub
