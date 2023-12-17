#include <cstdint>
#include "binder/bound_expression.h"
#include "catalog/schema.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"
#include "type/value_factory.h"

namespace bustub {
auto Optimizer::RewriteExpressionForAgg(const AbstractExpressionRef &expr, std::map<uint32_t, uint32_t> idx_map)
    -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(RewriteExpressionForAgg(child, idx_map));
  }
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      column_value_expr != nullptr) {
    BUSTUB_ENSURE(column_value_expr->GetTupleIdx() == 0, "tuple_idx cannot be value other than 0 before this stage.")
    auto col_idx = column_value_expr->GetColIdx();
    return std::make_shared<ColumnValueExpression>(0, idx_map[col_idx], column_value_expr->GetReturnType());
  }
  return expr->CloneWithChildren(children);
}
void Optimizer::ExtractProjectionIdx(const AbstractExpressionRef &expr, std::map<uint32_t, uint32_t> &idx_map) {
  for (const auto &child : expr->GetChildren()) {
    ExtractProjectionIdx(child, idx_map);
  }
  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      column_value_expr != nullptr) {
    auto col_idx = column_value_expr->GetColIdx();
    idx_map[col_idx] = col_idx;
  }
}

auto Optimizer::GetColumnIdx(const ColumnValueExpression *column_expr, size_t left_column_cnt, size_t right_column_cnt)
    -> uint32_t {
  if (column_expr->GetTupleIdx() == 1) {
    return column_expr->GetColIdx();
  }
  auto col_idx = column_expr->GetColIdx();
  if (col_idx < left_column_cnt) {
    return col_idx;
  }
  if (col_idx >= left_column_cnt && col_idx < left_column_cnt + right_column_cnt) {
    return col_idx - left_column_cnt;
  }
  throw bustub::Exception("col_idx not in range");
}

auto Optimizer::GetColumnTdx(const ColumnValueExpression *column_expr, size_t left_column_cnt, size_t right_column_cnt)
    -> uint32_t {
  if (column_expr->GetTupleIdx() == 1) {
    return 1;
  }
  auto col_idx = column_expr->GetColIdx();
  if (col_idx < left_column_cnt) {
    return 0;
  }
  if (col_idx >= left_column_cnt && col_idx < left_column_cnt + right_column_cnt) {
    return 1;
  }
  throw bustub::Exception("col_idx not in range");
}

void Optimizer::ReBuildFilterPredicate(const AbstractExpressionRef &expr,
                                       std::vector<AbstractExpressionRef> &predicates, LogicType type,
                                       size_t left_column_cnt, size_t right_column_cnt) {
  const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (cmp_expr == nullptr) {
    for (auto &child : expr->children_) {
      if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get()); logic_expr != nullptr) {
        ReBuildFilterPredicate(child, predicates, logic_expr->logic_type_, left_column_cnt, right_column_cnt);
      }
    }
    return;
  }
  const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
  const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
  if (left_expr == nullptr || right_expr == nullptr) {  // Column Constant Comparison
    const auto *column_value_expr = left_expr != nullptr ? left_expr : right_expr;
    AbstractExpressionRef constant_value_expr = left_expr != nullptr ? expr->children_[1] : expr->children_[0];
    auto tdx = GetColumnTdx(column_value_expr, left_column_cnt, right_column_cnt);
    if (tdx == 0) {
      predicates[1] = BuildFilterExpression(predicates[1], expr, type);
    } else {
      AbstractExpressionRef column_expr = std::make_shared<ColumnValueExpression>(
          0, GetColumnIdx(column_value_expr, left_column_cnt, right_column_cnt), column_value_expr->GetReturnType());
      AbstractExpressionRef new_cmp =
          std::make_shared<ComparisonExpression>(column_expr, constant_value_expr, cmp_expr->comp_type_);
      predicates[2] = BuildFilterExpression(predicates[2], new_cmp, type);
    }
  } else {  // Column Column Comparison
    auto left_tdx = GetColumnTdx(left_expr, left_column_cnt, right_column_cnt);
    auto right_tdx = GetColumnTdx(right_expr, left_column_cnt, right_column_cnt);
    if (left_tdx == 0 && right_tdx == 0) {
      predicates[1] = BuildFilterExpression(predicates[1], expr, type);
    } else if (left_tdx == 1 && right_tdx == 1) {
      AbstractExpressionRef left_column_expr = std::make_shared<ColumnValueExpression>(
          0, GetColumnIdx(left_expr, left_column_cnt, right_column_cnt), left_expr->GetReturnType());
      AbstractExpressionRef right_column_expr = std::make_shared<ColumnValueExpression>(
          0, GetColumnIdx(right_expr, left_column_cnt, right_column_cnt), right_expr->GetReturnType());
      AbstractExpressionRef new_cmp =
          std::make_shared<ComparisonExpression>(left_column_expr, right_column_expr, cmp_expr->comp_type_);
      predicates[2] = BuildFilterExpression(predicates[2], new_cmp, type);
    } else {
      predicates[0] = BuildFilterExpression(predicates[0], expr, type);
    }
  }
}

auto Optimizer::ExtractFilterKey(const AbstractExpressionRef &expr,
                                 std::unordered_map<uint32_t, bool> &filter_column_ids) -> bool {
  const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (cmp_expr == nullptr) {
    if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get()); logic_expr != nullptr) {
      if (logic_expr->logic_type_ == LogicType::Or) {
        return false;
      }
      for (auto &child : expr->children_) {
        if (!ExtractFilterKey(child, filter_column_ids)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
  const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
  const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
  if (cmp_expr->comp_type_ != ComparisonType::NotEqual) {
    if ((left_expr == nullptr && right_expr != nullptr) || (left_expr != nullptr && right_expr == nullptr)) {
      uint32_t idx = left_expr == nullptr ? right_expr->GetColIdx() : left_expr->GetColIdx();
      filter_column_ids[idx] = true;
      return true;
    }
  }
  return false;
}

auto Optimizer::OptimizePushDownPredicateScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePushDownPredicateScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
      const auto *table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
      const auto indices = catalog_.GetTableIndexes(table_info->name_);
      std::unordered_map<uint32_t, bool> filter_column_ids;
      auto &predict = filter_plan.GetPredicate();
      if (!ExtractFilterKey(predict, filter_column_ids)) {
        if (seq_scan_plan.filter_predicate_ == nullptr) {
          return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                   seq_scan_plan.table_name_, filter_plan.GetPredicate());
        }
        return optimized_plan;
      }
      for (const auto *index : indices) {
        const auto &columns = index->index_->GetKeyAttrs();
        // check index key schema == filter key columns
        size_t column_size = columns.size();
        if (column_size == filter_column_ids.size()) {
          bool valid = true;
          for (auto i : columns) {
            if (!filter_column_ids[i]) {
              valid = false;
              break;
            }
          }
          if (valid) {
            return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, index->index_oid_, predict);
          }
        }
      }
      if (seq_scan_plan.filter_predicate_ == nullptr) {
        return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                 seq_scan_plan.table_name_, filter_plan.GetPredicate());
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizePruningColumnAgg(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeProjection(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    // Has exactly one child
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Projection with multiple children?? That's weird!");
    const auto &child_plan = projection_plan.GetChildPlan();
    if (child_plan->GetType() == PlanType::Aggregation) {
      const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(*child_plan);
      BUSTUB_ENSURE(child_plan->children_.size() == 1, "Aggregation with multiple children?? That's weird!");
      auto &projection_expressions = projection_plan.expressions_;
      std::map<uint32_t, uint32_t> idx_map;
      for (auto &expr : projection_expressions) {
        ExtractProjectionIdx(expr, idx_map);
      }
      auto exprs_size = idx_map.size();
      auto group_size = agg_plan.group_bys_.size();
      std::vector<AggregationType> agg_types;
      std::vector<AbstractExpressionRef> aggregates;
      std::vector<Column> agg_columns;
      agg_types.reserve(exprs_size - group_size);
      aggregates.reserve(exprs_size - group_size);
      agg_columns.reserve(exprs_size);
      auto &old_agg_types = agg_plan.agg_types_;
      auto &old_aggregates = agg_plan.aggregates_;
      auto &agg_schema = agg_plan.output_schema_;
      for (uint32_t i = 0; i < group_size; i++) {
        agg_columns.push_back(agg_schema->GetColumn(i));
      }
      uint32_t new_idx = 0;
      for (auto &col : idx_map) {
        auto col_idx = col.first;
        if (col_idx < group_size) {
          continue;
        }
        auto agg_idx = col_idx - group_size;
        std::pair<AggregationType, AbstractExpressionRef> agg_pair = {old_agg_types[agg_idx], old_aggregates[agg_idx]};
        if (auto idx = AggItemExist(agg_types, aggregates, agg_pair); idx != -1) {
          col.second = idx + group_size;
        } else {
          agg_types.push_back(agg_pair.first);
          aggregates.push_back(agg_pair.second);
          agg_columns.emplace_back(fmt::format("agg#{}", new_idx), agg_schema->GetColumn(col_idx));
          col.second = new_idx + group_size;
          new_idx++;
        }
      }
      std::vector<AbstractExpressionRef> new_exprs;
      new_exprs.reserve(projection_expressions.size());
      for (auto &expr : projection_expressions) {
        new_exprs.emplace_back(RewriteExpressionForAgg(expr, idx_map));
      }
      auto optimized_agg_plan = std::make_shared<AggregationPlanNode>(
          std::make_shared<Schema>(agg_columns), agg_plan.GetChildPlan(), agg_plan.group_bys_, aggregates, agg_types);
      return std::make_shared<ProjectionPlanNode>(projection_plan.output_schema_, new_exprs, optimized_agg_plan);
    }
  }
  return optimized_plan;
}

auto Optimizer::BuildFilterExpression(AbstractExpressionRef &left_expr, const AbstractExpressionRef &right_expr,
                                      LogicType type) -> AbstractExpressionRef {
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(left_expr.get()); const_expr != nullptr) {
    return right_expr;
  }
  return std::make_shared<LogicExpression>(left_expr, right_expr, type);
}

auto Optimizer::GetBoolFromPredicate(const AbstractExpressionRef &expr) -> bool {
  const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (cmp_expr == nullptr) {
    if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get()); logic_expr != nullptr) {
      auto type = logic_expr->logic_type_;
      bool left_result = GetBoolFromPredicate(logic_expr->GetChildAt(0));
      bool right_result = GetBoolFromPredicate(logic_expr->GetChildAt(1));
      return type == LogicType::And ? left_result && right_result : left_result || right_result;
    }
    return true;
  }
  const auto *left_expr = dynamic_cast<const ConstantValueExpression *>(expr->children_[0].get());
  const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(expr->children_[1].get());
  if (left_expr == nullptr || right_expr == nullptr) {
    return true;
  }
  auto &left_val = left_expr->val_;
  auto &right_val = right_expr->val_;
  switch (cmp_expr->comp_type_) {
    case ComparisonType::Equal:
      return left_val.CompareEquals(right_val) == CmpBool::CmpTrue;
    case ComparisonType::NotEqual:
      return left_val.CompareNotEquals(right_val) == CmpBool::CmpTrue;
    case ComparisonType::LessThan:
      return left_val.CompareLessThan(right_val) == CmpBool::CmpTrue;
    case ComparisonType::LessThanOrEqual:
      return left_val.CompareLessThanEquals(right_val) == CmpBool::CmpTrue;
    case ComparisonType::GreaterThan:
      return left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue;
    case ComparisonType::GreaterThanOrEqual:
      return left_val.CompareGreaterThanEquals(right_val) == CmpBool::CmpTrue;
  }
}

auto Optimizer::AggItemExist(std::vector<AggregationType> &agg_types, std::vector<AbstractExpressionRef> &aggregates,
                             std::pair<AggregationType, AbstractExpressionRef> &agg_pair) -> int {
  uint32_t size = agg_types.size();
  auto &agg_type = agg_pair.first;
  auto &aggregate = agg_pair.second;
  for (uint32_t idx = 0; idx < size; idx++) {
    if (agg_types[idx] == agg_type) {
      auto &value = aggregates[idx];
      if (Equal(aggregate, value)) {
        return idx;
      }
    }
  }
  return -1;
}

auto Optimizer::Equal(AbstractExpressionRef &left_expr, AbstractExpressionRef &right_expr) -> bool {
  // TODO(su): Expression tree Comparison
  return left_expr->ToString() == right_expr->ToString();
}

auto Optimizer::OptimizeEliminateFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateFalseFilter(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (!GetBoolFromPredicate(filter_plan.GetPredicate())) {
      return std::make_shared<ValuesPlanNode>(std::make_shared<Schema>(std::vector<Column>{}),
                                              std::vector<std::vector<AbstractExpressionRef>>{});
    }
  }

  return optimized_plan;
}

auto Optimizer::OptimizeMergeDummyNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeDummyNLJ(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 2, "NLJ should have exactly 2 children.");
    auto left_plan = nlj_plan.GetLeftPlan();
    auto right_plan = nlj_plan.GetRightPlan();
    if (left_plan->GetType() == PlanType::Values) {
      const auto &value_plan = dynamic_cast<const ValuesPlanNode &>(*left_plan);
      if (value_plan.values_.empty()) {
        return optimized_plan->children_[1];
      }
    } else if (right_plan->GetType() == PlanType::Values) {
      const auto &value_plan = dynamic_cast<const ValuesPlanNode &>(*right_plan);
      if (value_plan.values_.empty()) {
        return optimized_plan->children_[0];
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizePushDownPredicateNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  AbstractPlanNodeRef optimized_plan = plan;
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    // Has exactly one child
    BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 2, "NLJ should have exactly 2 children.");
    auto &predicate = nlj_plan.predicate_;
    std::vector<AbstractExpressionRef> predicates;
    predicates.reserve(3);
    for (int i = 0; i < 3; i++) {
      predicates.push_back(std::make_shared<ConstantValueExpression>(ValueFactory::GetIntegerValue(0)));
    }
    AbstractPlanNodeRef left_plan = nlj_plan.GetLeftPlan();
    AbstractPlanNodeRef right_plan = nlj_plan.GetRightPlan();
    ReBuildFilterPredicate(predicate, predicates, LogicType::And, left_plan->OutputSchema().GetColumnCount(),
                           right_plan->OutputSchema().GetColumnCount());
    if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(predicates[1].get());
        const_expr == nullptr) {
      if (left_plan->GetType() == PlanType::NestedLoopJoin) {
        const auto &nlj_left_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*left_plan);
        left_plan = std::make_shared<NestedLoopJoinPlanNode>(left_plan->output_schema_, nlj_left_plan.GetLeftPlan(),
                                                             nlj_left_plan.GetRightPlan(), predicates[1],
                                                             nlj_left_plan.GetJoinType());
      } else {
        left_plan = std::make_shared<FilterPlanNode>(left_plan->output_schema_, predicates[1], left_plan);
      }
    }
    if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(predicates[2].get());
        const_expr == nullptr) {
      if (right_plan->GetType() == PlanType::NestedLoopJoin) {
        const auto &nlj_right_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*right_plan);
        right_plan = std::make_shared<NestedLoopJoinPlanNode>(right_plan->output_schema_, nlj_right_plan.GetLeftPlan(),
                                                              nlj_right_plan.GetRightPlan(), predicates[2],
                                                              nlj_right_plan.GetJoinType());
      } else {
        right_plan = std::make_shared<FilterPlanNode>(right_plan->output_schema_, predicates[2], right_plan);
      }
    }
    if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(predicates[0].get());
        const_expr == nullptr) {
      optimized_plan = std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, left_plan, right_plan,
                                                                predicates[0], nlj_plan.GetJoinType());
    }
  }
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : optimized_plan->GetChildren()) {
    children.emplace_back(OptimizePushDownPredicateNLJ(child));
  }
  optimized_plan = optimized_plan->CloneWithChildren(std::move(children));
  return optimized_plan;
}

auto Optimizer::OptimizeMergeSubProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeSubProjection(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    // Has exactly one child
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Projection with multiple children?? That's weird!");
    // If the schema is the same (except column name)
    const auto &child_plan = optimized_plan->children_[0];
    const auto &child_schema = child_plan->OutputSchema();
    const auto &projection_schema = projection_plan.OutputSchema();
    const auto &child_columns = child_schema.GetColumns();
    if (child_plan->GetType() == PlanType::Projection) {
      const auto &proj_child_plan = dynamic_cast<const ProjectionPlanNode &>(*child_plan);
      auto &projection_expressions = projection_plan.expressions_;
      auto &child_expressions = proj_child_plan.expressions_;
      std::vector<AbstractExpressionRef> expressions;
      std::vector<Column> columns;
      uint32_t size = projection_schema.GetColumnCount();
      expressions.reserve(size);
      for (uint32_t idx = 0; idx < size; idx++) {
        auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(projection_expressions[idx].get());
        if (column_value_expr == nullptr) {
          return optimized_plan;
        }
        expressions.emplace_back(child_expressions.at(column_value_expr->GetColIdx()));
        columns.emplace_back(child_columns.at(column_value_expr->GetColIdx()));
      }
      return std::make_shared<ProjectionPlanNode>(std::make_shared<Schema>(columns), expressions,
                                                  child_plan->GetChildAt(0));
    }
  }
  return optimized_plan;
}

}  // namespace bustub
