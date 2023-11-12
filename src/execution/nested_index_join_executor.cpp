//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <utility>
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), outer_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() == JoinType::INVALID || plan->GetJoinType() == JoinType::OUTER) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  auto exec_ctx = GetExecutorContext();
  auto cata_log = exec_ctx->GetCatalog();
  auto index_info = cata_log->GetIndex(plan_->index_oid_);
  inner_index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  table_info_ = cata_log->GetTable(plan_->inner_table_oid_);
  inner_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(inner_index_->GetBeginIterator());
  outer_executor_->Init();
  is_inner_exhausted_ = true;
  current_state_.first = Tuple{};
  current_state_.second = false;
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  switch (plan_->join_type_) {
    case JoinType::LEFT:
      return LeftJoinProcess(tuple, rid);
    case JoinType::RIGHT:
      return RightJoinProcess(tuple, rid);
    case JoinType::INNER:
      return InnerJoinProcess(tuple, rid);
    default:
      return false;
  }
}

auto NestIndexJoinExecutor::IndexNext(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (inner_iterator_->IsEnd()) {
      return false;
    }
    *rid = (*(*inner_iterator_)).second;
    auto tuple_info = table_info_->table_->GetTuple(*rid);
    if (tuple_info.first.is_deleted_) {
      ++(*inner_iterator_);
      continue;
    }
    *tuple = tuple_info.second;
    ++(*inner_iterator_);
    return true;
  }
}

auto NestIndexJoinExecutor::InnerJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = plan_->InnerTableSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);
  auto filter_expr = plan_->key_predicate_;
  auto &current_tuple = current_state_.first;

  while (true) {
    if (is_inner_exhausted_) {
      const auto status = outer_executor_->Next(&current_tuple, rid);
      if (!status) {
        return false;
      }
      is_inner_exhausted_ = false;
      current_state_.second = false;
    }

    if (!IndexNext(tuple, rid)) {
      inner_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(inner_index_->GetBeginIterator());
      is_inner_exhausted_ = true;
      continue;
    }
    auto value = filter_expr->EvaluateJoin(&current_tuple, outer_schema, tuple, inner_schema);
    if (!value.IsNull() && value.GetAs<bool>()) {
      for (uint32_t col = 0; col < outer_count; col++) {
        values.emplace_back(current_tuple.GetValue(&outer_schema, col));
      }
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(tuple->GetValue(&inner_schema, col));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      current_state_.second = true;
      return true;
    }
  }
}

auto NestIndexJoinExecutor::LeftJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = plan_->InnerTableSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);
  auto filter_expr = plan_->key_predicate_;
  auto &current_tuple = current_state_.first;

  while (true) {
    if (is_inner_exhausted_) {
      if (current_tuple.GetLength() != 0 && !current_state_.second) {
        for (uint32_t col = 0; col < outer_count; col++) {
          values.emplace_back(current_tuple.GetValue(&outer_schema, col));
        }
        for (uint32_t col = 0; col < inner_count; col++) {
          values.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        current_state_.second = true;
        return true;
      }
      const auto status = outer_executor_->Next(&current_tuple, rid);
      if (!status) {
        return false;
      }
      is_inner_exhausted_ = false;
      current_state_.second = false;
    }

    if (!IndexNext(tuple, rid)) {
      inner_iterator_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(inner_index_->GetBeginIterator());
      is_inner_exhausted_ = true;
      continue;
    }
    auto value = filter_expr->EvaluateJoin(&current_tuple, outer_schema, tuple, inner_schema);
    if (!value.IsNull() && value.GetAs<bool>()) {
      for (uint32_t col = 0; col < outer_count; col++) {
        values.emplace_back(current_tuple.GetValue(&outer_schema, col));
      }
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(tuple->GetValue(&inner_schema, col));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      current_state_.second = true;
      return true;
    }
  }
}

auto NestIndexJoinExecutor::RightJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = plan_->InnerTableSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);
  auto filter_expr = plan_->key_predicate_;
  auto &current_tuple = current_state_.first;

  while (true) {
    if (is_inner_exhausted_) {
      if (current_tuple.GetLength() != 0 && !current_state_.second) {
        for (uint32_t col = 0; col < outer_count; col++) {
          values.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
        }
        for (uint32_t col = 0; col < inner_count; col++) {
          values.emplace_back(current_tuple.GetValue(&inner_schema, col));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        current_state_.second = true;
        return true;
      }
      const auto status = IndexNext(&current_tuple, rid);
      if (!status) {
        return false;
      }
      is_inner_exhausted_ = false;
      current_state_.second = false;
    }

    if (!outer_executor_->Next(tuple, rid)) {
      outer_executor_->Init();
      is_inner_exhausted_ = true;
      continue;
    }
    auto value = filter_expr->EvaluateJoin(&current_tuple, inner_schema, tuple, outer_schema);
    if (!value.IsNull() && value.GetAs<bool>()) {
      for (uint32_t col = 0; col < outer_count; col++) {
        values.emplace_back(tuple->GetValue(&outer_schema, col));
      }
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(current_tuple.GetValue(&inner_schema, col));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      current_state_.second = true;
      return true;
    }
  }
}

}  // namespace bustub
