#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"

namespace bustub {

/**
 * The optimizer takes an `AbstractPlanNode` and outputs an optimized `AbstractPlanNode`.
 */
class Optimizer {
 public:
  explicit Optimizer(const Catalog &catalog, bool force_starter_rule)
      : catalog_(catalog), force_starter_rule_(force_starter_rule) {}

  auto Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

 private:
  /**
   * @brief merge projections that do identical project.
   * Identical projection might be produced when there's `SELECT *`, aggregation, or when we need to rename the columns
   * in the planner. We merge these projections so as to make execution faster.
   */
  auto OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeMergeSubProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizePruningColumnAgg(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter condition into nested loop join.
   * In planner, we plan cross join + filter with cross product (done with nested loop join) and a filter plan node. We
   * can merge the filter condition into nested loop join to achieve better efficiency.
   */
  auto OptimizeMergeFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeMergeDummyNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief push down constant filter condition into filter plan below nested loop join.
   */
  auto OptimizePushDownPredicateNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into hash join.
   * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
   * with multiple eq conditions.
   */
  auto OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into index join.
   */
  auto OptimizeNLJAsIndexJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief eliminate always true filter
   */
  auto OptimizeEliminateTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief eliminate always false filter
   */
  auto OptimizeEliminateFalseFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizePushDownPredicateScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;
  /**
   * @brief merge filter into filter_predicate of seq scan plan node
   */
  auto OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** @brief extract filter keys from the predict conditions */
  auto ExtractFilterKey(const AbstractExpressionRef &expr, std::unordered_map<uint32_t, bool> &filter_column_ids)
      -> bool;

  /**
   * @brief rewrite expression to be used in nested loop joins. e.g., if we have `SELECT * FROM a, b WHERE a.x = b.y`,
   * we will have `#0.x = #0.y` in the filter plan node. We will need to figure out where does `0.x` and `0.y` belong
   * in NLJ (left table or right table?), and rewrite it as `#0.x = #1.y`.
   *
   * @param expr the filter expression
   * @param left_column_cnt number of columns in the left size of the NLJ
   * @param right_column_cnt number of columns in the left size of the NLJ
   */
  auto RewriteExpressionForJoin(const AbstractExpressionRef &expr, size_t left_column_cnt, size_t right_column_cnt)
      -> AbstractExpressionRef;

  auto RewriteExpressionForAgg(const AbstractExpressionRef &expr, std::map<uint32_t, uint32_t> idx_map)
      -> AbstractExpressionRef;

  /** @brief check if the predicate is true::boolean */
  auto IsPredicateTrue(const AbstractExpressionRef &expr) -> bool;

  auto GetBoolFromPredicate(const AbstractExpressionRef &expr) -> bool;

  void ReBuildFilterPredicate(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &predicates,
                              LogicType type, size_t left_column_cnt, size_t right_column_cnt);

  auto BuildFilterExpression(AbstractExpressionRef &left_expr, const AbstractExpressionRef &right_expr, LogicType type)
      -> AbstractExpressionRef;

  auto GetColumnIdx(const ColumnValueExpression *column_expr, size_t left_column_cnt, size_t right_column_cnt)
      -> uint32_t;

  auto GetColumnTdx(const ColumnValueExpression *column_expr, size_t left_column_cnt, size_t right_column_cnt)
      -> uint32_t;

  void ExtractProjectionIdx(const AbstractExpressionRef &expr, std::map<uint32_t, uint32_t> &idx_map);
  /** @brief extract hash join keys from the predict conditions, check if expr is equal condition */
  auto ExtractJoinKey(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_key_expressions,
                      std::vector<AbstractExpressionRef> &right_key_expressions, size_t left_column_cnt,
                      size_t right_column_cnt) -> bool;

  auto AggItemExist(std::vector<AggregationType> &agg_types, std::vector<AbstractExpressionRef> &aggregates,
                    std::pair<AggregationType, AbstractExpressionRef> &agg_pair) -> int;

  auto Equal(AbstractExpressionRef &left_expr, AbstractExpressionRef &right_expr) -> bool;
  /**
   * @brief optimize order by as index scan if there's an index on a table
   */
  auto OptimizeOrderByAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** @brief check if the index can be matched */
  auto MatchIndex(const std::string &table_name, uint32_t index_key_idx)
      -> std::optional<std::tuple<index_oid_t, std::string>>;

  /**
   * @brief optimize sort + limit as top N
   */
  auto OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief get the estimated cardinality for a table based on the table name. Useful when join reordering. BusTub
   * doesn't support statistics for now, so it's the only way for you to get the table size :(
   *
   * @param table_name
   * @return std::optional<size_t>
   */
  auto EstimatedCardinality(const std::string &table_name) -> std::optional<size_t>;

  /** Catalog will be used during the planning process. USERS SHOULD ENSURE IT OUTLIVES
   * OPTIMIZER, otherwise it's a dangling reference.
   */
  const Catalog &catalog_;

  const bool force_starter_rule_;
};

}  // namespace bustub
