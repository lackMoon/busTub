#include "execution/executors/topn_executor.h"
#include <memory>
#include <queue>
#include <set>
#include <utility>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto &schema = plan_->OutputSchema();
  auto &order_bys = plan_->order_bys_;
  cmp_ = [&order_bys, &schema](const Tuple &lvalue, const Tuple &rvalue) {
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

void TopNExecutor::Init() {
  Tuple tuple{};
  RID rid;
  child_executor_->Init();
  size_ = 0;
  top_entries_ = std::make_unique<std::map<Tuple, int, decltype(cmp_)>>(cmp_);
  auto n = plan_->n_;
  while (child_executor_->Next(&tuple, &rid)) {
    Insert(tuple);
    if (size_ > n) {
      Pop(false);
    }
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (size_ == 0) {
    return false;
  }
  *tuple = Top();
  *rid = tuple->GetRid();
  Pop(true);
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return size_; };

}  // namespace bustub
