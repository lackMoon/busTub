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

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

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
    table_info_->table_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, *rid);
    for (auto index_info : index_set_) {
      auto &index = index_info->index_;
      auto key = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
      index->DeleteEntry(key, child_tuple.GetRid(), exec_ctx_->GetTransaction());
    }
  }
  *tuple = Tuple{{Value(TypeId::INTEGER, delete_rows)}, &plan_->OutputSchema()};
  is_done_ = true;
  return true;
}

}  // namespace bustub
