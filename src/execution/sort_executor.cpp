#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <utility>
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto &schema = plan_->OutputSchema();
  auto &order_bys = plan_->order_bys_;
  cmp_ = [&order_bys, &schema](Tuple &lvalue, Tuple &rvalue) {
    for (auto &order_by : order_bys) {
      auto &expr = order_by.second;
      auto lval = expr->Evaluate(&lvalue, schema);
      auto rval = expr->Evaluate(&rvalue, schema);
      if (lval.CompareEquals(rval) != CmpBool::CmpTrue) {
        return order_by.first == OrderByType::DESC ? lval.CompareGreaterThan(rval) == CmpBool::CmpTrue
                                                   : lval.CompareLessThan(rval) == CmpBool::CmpTrue;
      }
    }
    return true;
  };
}

void SortExecutor::Init() {
  Tuple tuple{};
  RID rid;
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    sort_tuples_.push_back(tuple);
  }
  std::sort(sort_tuples_.begin(), sort_tuples_.end(), cmp_);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sort_tuples_.empty()) {
    return false;
  }
  *tuple = sort_tuples_.front();
  *rid = tuple->GetRid();
  sort_tuples_.pop_front();
  return true;
}

}  // namespace bustub
