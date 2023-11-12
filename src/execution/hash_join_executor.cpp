//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <utility>
#include "binder/table_ref/bound_join_ref.h"
#include "execution/plans/hash_join_plan.h"
#include "primer/trie.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      outer_executor_(std::move(left_child)),
      inner_executor_(std::move(right_child)),
      jht_iterator_(jht_.Begin()) {
  if (plan->GetJoinType() == JoinType::INVALID) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  Tuple tuple{};
  RID rid;
  outer_executor_->Init();
  inner_executor_->Init();
  jht_.Clear();
  auto &schema = outer_executor_->GetOutputSchema();
  while (outer_executor_->Next(&tuple, &rid)) {
    jht_.Insert(MakeJoinKey(&tuple, true), MakeJoinValue(&tuple, schema));
  }
  jht_iterator_ = jht_.Begin();
  current_tuple_ = Tuple{};
  to_next_ = true;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  switch (plan_->join_type_) {
    case JoinType::LEFT:
      return LeftJoinProcess(tuple, rid);
    case JoinType::RIGHT:
      return RightJoinProcess(tuple, rid);
    case JoinType::INNER:
      return InnerJoinProcess(tuple, rid);
    case JoinType::OUTER:
      return OuterJoinProcess(tuple, rid);
    default:
      return false;
  }
}

auto HashJoinExecutor::InnerJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = inner_executor_->GetOutputSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);

  while (true) {
    if (to_next_ && !inner_executor_->Next(&current_tuple_, rid)) {
      return false;
    }

    if (int result = jht_.Combine(MakeJoinKey(&current_tuple_, false), values);
        result != SimpleJoinHashTable::UNMATCH) {
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(current_tuple_.GetValue(&inner_schema, col));
      }
      to_next_ = (result == SimpleJoinHashTable::EXHAUSTED);
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }
}

auto HashJoinExecutor::LeftJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = inner_executor_->GetOutputSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);

  while (true) {
    if (to_next_ && !inner_executor_->Next(&current_tuple_, rid)) {
      break;
    }

    if (int result = jht_.Combine(MakeJoinKey(&current_tuple_, false), values);
        result != SimpleJoinHashTable::UNMATCH) {
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(current_tuple_.GetValue(&inner_schema, col));
      }
      to_next_ = (result == SimpleJoinHashTable::EXHAUSTED);
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }

  while (jht_iterator_ != jht_.End()) {
    auto &join_item = jht_iterator_.Val();
    if (!join_item.is_match_) {
      auto &join_val = join_item.join_values_[join_item.cursor_++].values_;
      values.insert(values.end(), join_val.begin(), join_val.end());
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
      }
      if (jht_iterator_.IsExhausted()) {
        join_item.cursor_ = 0;
        ++jht_iterator_;
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    ++jht_iterator_;
  }
  return false;
}

auto HashJoinExecutor::RightJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = inner_executor_->GetOutputSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);

  while (true) {
    if (to_next_ && !inner_executor_->Next(&current_tuple_, rid)) {
      return false;
    }

    if (int result = jht_.Combine(MakeJoinKey(&current_tuple_, false), values);
        result != SimpleJoinHashTable::UNMATCH) {
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(current_tuple_.GetValue(&inner_schema, col));
      }
      to_next_ = (result == SimpleJoinHashTable::EXHAUSTED);
    } else {
      for (uint32_t col = 0; col < outer_count; col++) {
        values.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
      }
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(current_tuple_.GetValue(&inner_schema, col));
      }
    }
    *tuple = Tuple{values, &GetOutputSchema()};
    return true;
  }
}

auto HashJoinExecutor::OuterJoinProcess(Tuple *tuple, RID *rid) -> bool {
  auto outer_schema = outer_executor_->GetOutputSchema();
  auto inner_schema = inner_executor_->GetOutputSchema();
  auto outer_count = outer_schema.GetColumnCount();
  auto inner_count = inner_schema.GetColumnCount();
  std::vector<Value> values{};
  values.reserve(outer_count + inner_count);

  while (true) {
    if (to_next_ && !inner_executor_->Next(&current_tuple_, rid)) {
      break;
    }

    if (int result = jht_.Combine(MakeJoinKey(&current_tuple_, false), values);
        result != SimpleJoinHashTable::UNMATCH) {
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(current_tuple_.GetValue(&inner_schema, col));
      }
      to_next_ = (result == SimpleJoinHashTable::EXHAUSTED);
    } else {
      for (uint32_t col = 0; col < outer_count; col++) {
        values.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
      }
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(current_tuple_.GetValue(&inner_schema, col));
      }
    }
    *tuple = Tuple{values, &GetOutputSchema()};
    return true;
  }

  while (jht_iterator_ != jht_.End()) {
    auto &join_item = jht_iterator_.Val();
    if (!join_item.is_match_) {
      auto &join_val = join_item.join_values_[join_item.cursor_++].values_;
      values.insert(values.end(), join_val.begin(), join_val.end());
      for (uint32_t col = 0; col < inner_count; col++) {
        values.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
      }
      if (jht_iterator_.IsExhausted()) {
        join_item.cursor_ = 0;
        ++jht_iterator_;
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    ++jht_iterator_;
  }
  return false;
}

}  // namespace bustub
