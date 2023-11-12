#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::ExtractJoinKey(const AbstractExpressionRef &expr,
                               std::vector<AbstractExpressionRef> &left_key_expressions,
                               std::vector<AbstractExpressionRef> &right_key_expressions, size_t left_column_cnt,
                               size_t right_column_cnt) -> bool {
  const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (cmp_expr == nullptr) {
    for (auto &child : expr->children_) {
      if (!ExtractJoinKey(child, left_key_expressions, right_key_expressions, left_column_cnt, right_column_cnt)) {
        return false;
      }
    }
    return true;
  }
  const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
  const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
  if (left_expr == nullptr || right_expr == nullptr) {
    return false;
  }
  if (cmp_expr->comp_type_ != ComparisonType::Equal) {
    return false;
  }
  if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 0) {
    left_key_expressions.emplace_back(std::make_shared<ColumnValueExpression>(
        0, GetColumnIdx(left_expr, left_column_cnt, right_column_cnt), left_expr->GetReturnType()));
    right_key_expressions.emplace_back(std::make_shared<ColumnValueExpression>(
        0, GetColumnIdx(right_expr, left_column_cnt, right_column_cnt), right_expr->GetReturnType()));
  }
  if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
    left_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
    right_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
  }
  if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
    left_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
    right_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
  }
  return true;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    if (IsPredicateTrue(nlj_plan.Predicate())) {
      return optimized_plan;
    }
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    if (ExtractJoinKey(nlj_plan.Predicate(), left_key_expressions, right_key_expressions,
                       nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount(),
                       nlj_plan.GetRightPlan()->OutputSchema().GetColumnCount())) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left_key_expressions, right_key_expressions,
                                                nlj_plan.join_type_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
