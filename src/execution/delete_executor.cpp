//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx->GetTransaction()) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  auto exec_ctx = GetExecutorContext();
  auto cata_log = exec_ctx->GetCatalog();
  table_info_ = cata_log->GetTable(plan_->table_oid_);
  index_set_ = cata_log->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  int delete_rows;
  Tuple child_tuple{};

  for (delete_rows = 0; child_executor_->Next(&child_tuple, rid); delete_rows++) {
    auto oid = plan_->table_oid_;
    table_info_->table_->UpdateTupleMeta({INVALID_TXN_ID, txn_->GetTransactionId(), true}, *rid);
    txn_->AppendTableWriteRecord({oid, *rid, table_info_->table_.get()});
    for (auto index_info : index_set_) {
      auto &index = index_info->index_;
      auto key = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
      txn_->AppendIndexWriteRecord(
          {child_tuple.GetRid(), oid, WType::DELETE, key, index_info->index_oid_, exec_ctx_->GetCatalog()});
      index->DeleteEntry(key, child_tuple.GetRid(), exec_ctx_->GetTransaction());
    }
  }
  *tuple = Tuple{{Value(TypeId::INTEGER, delete_rows)}, &plan_->OutputSchema()};
  is_done_ = true;
  return true;
}

}  // namespace bustub
