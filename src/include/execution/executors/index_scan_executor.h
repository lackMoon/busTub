//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
  using LockMode = LockManager::LockMode;

 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  void ExtractKeyRangeFromPredict(const AbstractExpressionRef &predicate,
                                  std::unordered_map<uint32_t, IndexKeyRange> &key_range);

  auto GetKey(const IntegerKeyType &key, Schema *key_schema, uint32_t count) -> Tuple {
    std::vector<Value> values;
    for (uint32_t i = 0; i < count; i++) {
      values.push_back(key.ToValue(key_schema, i));
    }
    return Tuple{values, key_schema};
  }

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

  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  TableInfo *table_info_;

  BPlusTreeIndexForTwoIntegerColumn *index_;

  std::unique_ptr<BPlusTreeIndexIteratorForTwoIntegerColumn> begin_iterator_;

  std::unique_ptr<BPlusTreeIndexIteratorForTwoIntegerColumn> end_iterator_;

  Transaction *txn_;

  LockManager *lock_mgr_;

  bool is_deleted_;
};
}  // namespace bustub
