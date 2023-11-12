//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/** JoinKey represents a key in an join operation */
struct JoinKey {
  /** The join keys */
  std::vector<Value> keys_;

  /**
   * Compares two join keys for equality.
   * @param other the other join key to be compared with
   * @return `true` if both join keys have equivalent key expressions, `false` otherwise
   */
  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** JoinValue represents a value for each item of the hash table */
struct JoinValue {
  /** The join values */
  std::vector<Value> values_;
};

/** JoinItem represents a value for each record of the hash table */
struct JoinItem {
  /** return true if record matched the right child node */
  bool is_match_;
  /** The join values */
  std::vector<JoinValue> join_values_;
  /** The pointer of the current record */
  int cursor_;

  int size_;
};

}  // namespace bustub

namespace std {
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {
class SimpleJoinHashTable {
 public:
  static const int UNMATCH = -1;
  static const int EXHAUSTED = 0;
  static const int MATCH = 1;

  /**
   * Inserts a value into the hash table
   * @param join_key the key to be inserted
   * @param join_val the value to be inserted
   */
  void Insert(const JoinKey &join_key, const JoinValue &join_val) {
    if (ht_.count(join_key) == 0) {
      ht_.insert({join_key, {false, {join_val}, 0, 1}});
    } else {
      auto &join_item = ht_.at(join_key);
      join_item.join_values_.emplace_back(join_val);
      join_item.size_++;
    }
  }

  /**
   * Returns true and combines tuple'svalue if key is equal.
   * @param join_key the key to be inserted
   * @param vals the value to be inserted
   */
  auto Combine(const JoinKey &join_key, std::vector<Value> &vals) -> int {
    if (ht_.count(join_key) == 0) {
      return UNMATCH;
    }
    auto &join_item = ht_.at(join_key);
    join_item.is_match_ = true;
    auto &join_val = join_item.join_values_[join_item.cursor_++].values_;
    vals.insert(vals.end(), join_val.begin(), join_val.end());
    if (join_item.cursor_ >= join_item.size_) {
      join_item.cursor_ = 0;
      return EXHAUSTED;
    }
    return MATCH;
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /**
   * Returns true if the hash table is empty.
   */
  auto Empty() -> bool { return ht_.empty(); }

  /** An iterator over the join hash table */
  class Iterator {
   public:
    /** Creates an iterator for the join map. */
    explicit Iterator(std::unordered_map<JoinKey, JoinItem>::iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const JoinKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> JoinItem & { return iter_->second; }

    auto IsExhausted() -> bool { return iter_->second.cursor_ >= iter_->second.size_; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Joins map */
    std::unordered_map<JoinKey, JoinItem>::iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.begin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.end()}; }

 private:
  /** The hash table is just a map from join keys to join values */
  std::unordered_map<JoinKey, JoinItem> ht_;
};

/**
 * HashJoinExecutor executes a hash JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto InnerJoinProcess(Tuple *tuple, RID *rid) -> bool;

  auto LeftJoinProcess(Tuple *tuple, RID *rid) -> bool;

  auto RightJoinProcess(Tuple *tuple, RID *rid) -> bool;

  auto OuterJoinProcess(Tuple *tuple, RID *rid) -> bool;

  /** @return The tuple as an JoinKey */
  auto MakeJoinKey(const Tuple *tuple, bool is_left) -> JoinKey {
    std::vector<Value> keys;
    auto &exprs = is_left ? plan_->LeftJoinKeyExpressions() : plan_->RightJoinKeyExpressions();
    auto &schema = is_left ? plan_->GetLeftPlan()->OutputSchema() : plan_->GetRightPlan()->OutputSchema();
    keys.reserve(exprs.size());
    for (const auto &expr : exprs) {
      keys.emplace_back(expr->Evaluate(tuple, schema));
    }
    return {keys};
  }

  /** @return The tuple as an JoinValue */
  auto MakeJoinValue(const Tuple *tuple, const Schema &schema) -> JoinValue {
    std::vector<Value> vals;
    auto cols = schema.GetColumnCount();
    vals.reserve(cols);
    for (uint32_t i = 0; i < cols; i++) {
      vals.emplace_back(tuple->GetValue(&schema, i));
    }
    return {vals};
  }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> outer_executor_;

  std::unique_ptr<AbstractExecutor> inner_executor_;

  SimpleJoinHashTable jht_;

  SimpleJoinHashTable::Iterator jht_iterator_;

  Tuple current_tuple_;

  bool to_next_;
};

}  // namespace bustub
