//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "storage/index/generic_key.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"
#include "type/limits.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      txn_(exec_ctx->GetTransaction()),
      lock_mgr_(exec_ctx_->GetLockManager()),
      is_deleted_(exec_ctx->IsDelete()) {}

void IndexScanExecutor::ExtractKeyRangeFromPredict(const AbstractExpressionRef &predicate,
                                                   std::unordered_map<uint32_t, IndexKeyRange> &key_range) {
  const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(predicate.get());
  if (cmp_expr == nullptr) {
    for (auto &child : predicate->children_) {
      ExtractKeyRangeFromPredict(child, key_range);
    }
    return;
  }
  const auto *child_expr = dynamic_cast<const ColumnValueExpression *>(predicate->children_[0].get());
  const auto *key_expr =
      child_expr != nullptr ? child_expr : dynamic_cast<const ColumnValueExpression *>(predicate->children_[1].get());
  const auto *const_expr = child_expr == nullptr
                               ? dynamic_cast<const ConstantValueExpression *>(predicate->children_[0].get())
                               : dynamic_cast<const ConstantValueExpression *>(predicate->children_[1].get());
  auto idx = key_expr->GetColIdx();
  auto value = const_expr->val_;
  auto &range = key_range[idx];
  switch (cmp_expr->comp_type_) {
    case ComparisonType::Equal:
      if (value.CompareLessThan(range.from_) == CmpBool::CmpTrue ||
          value.CompareGreaterThan(range.to_) == CmpBool::CmpTrue) {
        range.is_valid_ = false;
      } else {
        range.from_ = value;
        range.to_ = value;
      }
      break;
    case ComparisonType::LessThan:
      value.Subtract(ValueFactory::GetIntegerValue(1));
    case ComparisonType::LessThanOrEqual:
      if (value.CompareLessThan(range.from_) == CmpBool::CmpTrue) {
        range.is_valid_ = false;
      } else if (value.CompareLessThan(range.to_) == CmpBool::CmpTrue) {
        range.to_ = value;
      }
      break;
    case ComparisonType::GreaterThan:
      value.Add(ValueFactory::GetIntegerValue(1));
    case ComparisonType::GreaterThanOrEqual:
      if (value.CompareGreaterThan(range.to_) == CmpBool::CmpTrue) {
        range.is_valid_ = false;
      } else if (value.CompareGreaterThan(range.from_) == CmpBool::CmpTrue) {
        range.from_ = value;
      }
      break;
    case ComparisonType::NotEqual:
      break;
  }
}

void IndexScanExecutor::Init() {
  auto exec_ctx = GetExecutorContext();
  auto cata_log = exec_ctx->GetCatalog();
  auto index_info = cata_log->GetIndex(plan_->index_oid_);
  index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  auto index_schema = index_->GetKeySchema();
  auto oid = cata_log->GetTable(index_info->table_name_)->oid_;
  TryLockTable(oid);
  table_info_ = cata_log->GetTable(oid);
  if (plan_->predicate_) {
    std::unordered_map<uint32_t, IndexKeyRange> key_range;
    for (auto &idx : index_->GetKeyAttrs()) {
      key_range[idx] = {ValueFactory::GetIntegerValue(BUSTUB_INT32_MIN),
                        ValueFactory::GetIntegerValue(BUSTUB_INT32_MAX), true};
    }
    ExtractKeyRangeFromPredict(plan_->predicate_, key_range);
    std::vector<Value> start;
    std::vector<Value> end;
    for (auto &idx : index_->GetKeyAttrs()) {
      auto &range = key_range[idx];
      if (!range.is_valid_) {
        begin_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(index_->GetBeginIterator());
        end_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(index_->GetBeginIterator());
        return;
      }
      start.push_back(range.from_);
      end.push_back(range.to_);
    }
    GenericKey<TWO_INTEGER_SIZE> begin_key;
    begin_key.SetFromKey(Tuple{start, index_schema});
    begin_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(index_->GetBeginIterator(begin_key));
    GenericKey<TWO_INTEGER_SIZE> end_key;
    end_key.SetFromKey(Tuple{end, index_schema});
    auto iter = index_->GetBeginIterator(end_key);
    ++iter;
    end_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(std::move(iter));
  } else {
    begin_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(index_->GetBeginIterator());
    end_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(index_->GetEndIterator());
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    auto oid = table_info_->oid_;
    if ((*begin_iterator_) == (*end_iterator_)) {
      TryUnLockTable(oid);
      return false;
    }
    if (plan_->predicate_) {
      auto key_schema = index_->GetKeySchema();
      auto key_tuple = GetKey((*(*begin_iterator_)).first, key_schema, index_->GetIndexColumnCount());
      auto value = plan_->predicate_->Evaluate(&key_tuple, *key_schema);
      if ((value.IsNull() || !value.GetAs<bool>())) {
        ++(*begin_iterator_);
        continue;
      }
    }
    *rid = (*(*begin_iterator_)).second;
    TryLockTuple(oid, *rid);
    auto tuple_info = table_info_->table_->GetTuple(*rid);
    if (tuple_info.first.is_deleted_) {
      TryUnLockTuple(oid, *rid, true);
      ++(*begin_iterator_);
      continue;
    }
    *tuple = tuple_info.second;
    TryUnLockTuple(oid, *rid);
    ++(*begin_iterator_);
    return true;
  }
}

}  // namespace bustub
