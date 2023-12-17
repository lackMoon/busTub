//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include <utility>
#include "catalog/schema.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      schema_({}),
      txn_(exec_ctx->GetTransaction()),
      lock_mgr_(exec_ctx->GetLockManager()),
      is_deleted_(exec_ctx->IsDelete()) {}

void SeqScanExecutor::Init() {
  auto exec_ctx = GetExecutorContext();
  auto cata_log = exec_ctx->GetCatalog();
  auto oid = plan_->table_oid_;
  TryLockTable(oid);
  auto table = cata_log->GetTable(oid);
  schema_ = table->schema_;
  if (table == Catalog::NULL_TABLE_INFO) {
    throw bustub::Exception(fmt::format("Table {} doesn't exist", plan_->table_name_));
  }
  iterator_ = std::make_unique<TableIterator>(table->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    auto oid = plan_->table_oid_;
    if (iterator_->IsEnd()) {
      // Scan complete
      TryUnLockTable(oid);
      return false;
    }
    auto curr_rid = iterator_->GetRID();
    TryLockTuple(oid, curr_rid);
    auto tuple_info = iterator_->GetTuple();
    if (tuple_info.first.is_deleted_) {
      TryUnLockTuple(oid, curr_rid, true);
      ++(*iterator_);
      continue;
    }
    *tuple = tuple_info.second;
    if (auto &filter_expr = plan_->filter_predicate_; filter_expr != nullptr) {
      auto value = filter_expr->Evaluate(tuple, schema_);
      if ((value.IsNull() || !value.GetAs<bool>())) {
        TryUnLockTuple(oid, curr_rid, true);
        ++(*iterator_);
        continue;
      }
    }
    *rid = curr_rid;
    TryUnLockTuple(oid, curr_rid);
    ++(*iterator_);
    return true;
  }
}

}  // namespace bustub
