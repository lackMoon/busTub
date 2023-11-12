//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <utility>
#include "binder/bound_expression.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      outer_executor_(std::move(left_executor)),
      inner_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() == JoinType::INVALID || plan->GetJoinType() == JoinType::OUTER) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  outer_executor_->Init();
  inner_executor_->Init();
  is_inner_exhausted_ = true;
  current_state_.first = Tuple{};
  current_state_.second = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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

auto NestedLoopJoinExecutor::InnerJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = inner_executor_->GetOutputSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);
  auto filter_expr = plan_->predicate_;
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

    if (!inner_executor_->Next(tuple, rid)) {
      inner_executor_->Init();
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

auto NestedLoopJoinExecutor::LeftJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = inner_executor_->GetOutputSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);
  auto filter_expr = plan_->predicate_;
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

    if (!inner_executor_->Next(tuple, rid)) {
      inner_executor_->Init();
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

auto NestedLoopJoinExecutor::RightJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = inner_executor_->GetOutputSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);
  auto filter_expr = plan_->predicate_;
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
      const auto status = inner_executor_->Next(&current_tuple, rid);
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
