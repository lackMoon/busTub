//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdint>
#include <memory>
#include <ostream>
#include <utility>

#include "execution/executors/update_executor.h"
#include "storage/table/table_heap.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  auto exec_ctx = GetExecutorContext();
  auto cata_log = exec_ctx->GetCatalog();
  table_info_ = cata_log->GetTable(plan_->table_oid_);
  index_set_ = cata_log->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Match(const Tuple &delete_tuple, const Tuple &insert_tuple, const Schema &key_schema) -> bool {
  auto count = key_schema.GetColumnCount();
  for (uint32_t idx = 0; idx < count; idx++) {
    Value delete_value = delete_tuple.GetValue(&key_schema, idx);
    Value insert_value = insert_tuple.GetValue(&key_schema, idx);
    if (delete_value.CompareNotEquals(insert_value) == CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }
  int update_rows;
  Tuple child_tuple{};
  Tuple new_tuple{};

  for (update_rows = 0; child_executor_->Next(&child_tuple, rid); update_rows++) {
    // Compute expressions
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    new_tuple = Tuple{values, &table_info_->schema_};
    table_info_->table_->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, true}, *rid);
    RID new_rid = table_info_->table_->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple).value();
    for (auto index_info : index_set_) {
      auto &index = index_info->index_;
      auto delete_key = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
      auto insert_key = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs());
      if (!Match(delete_key, insert_key, index_info->key_schema_)) {
        index->DeleteEntry(delete_key, *rid, exec_ctx_->GetTransaction());
        index->InsertEntry(insert_key, new_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  *tuple = Tuple{{Value(TypeId::INTEGER, update_rows)}, &plan_->OutputSchema()};
  is_done_ = true;
  return true;
}

}  // namespace bustub
