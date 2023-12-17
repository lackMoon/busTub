//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>

#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx->GetTransaction()),
      lock_mgr_(exec_ctx->GetLockManager()) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  auto exec_ctx = GetExecutorContext();
  auto cata_log = exec_ctx->GetCatalog();
  auto oid = plan_->table_oid_;
  auto lock_mode = LockMode::INTENTION_EXCLUSIVE;
  if (!(txn_->IsTableExclusiveLocked(oid) || txn_->IsTableIntentionExclusiveLocked(oid))) {
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
  table_info_ = cata_log->GetTable(oid);
  index_set_ = cata_log->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  int insert_rows;
  Tuple child_tuple{};
  auto oid = plan_->table_oid_;
  for (insert_rows = 0; child_executor_->Next(&child_tuple, rid); insert_rows++) {
    *rid =
        table_info_->table_
            ->InsertTuple(TupleMeta{txn_->GetTransactionId(), INVALID_TXN_ID, false}, child_tuple, lock_mgr_, txn_, oid)
            .value();
    txn_->AppendTableWriteRecord({oid, *rid, table_info_->table_.get()});
    for (auto index_info : index_set_) {
      auto &index = index_info->index_;
      auto key = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
      txn_->AppendIndexWriteRecord({*rid, oid, WType::INSERT, key, index_info->index_oid_, exec_ctx_->GetCatalog()});
      index->InsertEntry(key, *rid, txn_);
    }
  }
  *tuple = Tuple{{Value(TypeId::INTEGER, insert_rows)}, &plan_->OutputSchema()};
  is_done_ = true;
  return true;
}

}  // namespace bustub
