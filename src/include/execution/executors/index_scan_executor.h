//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  void ExtractKeyRangeFromPredict(const AbstractExpressionRef &predicate,
                                  std::unordered_map<uint32_t, IndexKeyRange> &key_range);

  auto GetKey(const IntegerKeyType &key, Schema *key_schema, uint32_t count) -> Tuple {
    std::vector<Value> values;
    for (uint32_t i = 0; i < count; i++) {
      values.push_back(key.ToValue(key_schema, i));
    }
    return Tuple{values, key_schema};
  }
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  TableInfo *table_info_;

  BPlusTreeIndexForTwoIntegerColumn *index_;

  std::unique_ptr<BPlusTreeIndexIteratorForTwoIntegerColumn> begin_iterator_;

  std::unique_ptr<BPlusTreeIndexIteratorForTwoIntegerColumn> end_iterator_;
};
}  // namespace bustub
