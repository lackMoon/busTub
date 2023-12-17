//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto table_write_set = txn->GetWriteSet();
  auto index_write_set = txn->GetIndexWriteSet();
  while (!table_write_set->empty()) {
    auto &item = table_write_set->back();
    auto *table_heap = item.table_heap_;
    auto &rid = item.rid_;
    auto meta = table_heap->GetTupleMeta(rid);
    table_heap->UpdateTupleMeta({INVALID_TXN_ID, INVALID_TXN_ID, !meta.is_deleted_}, rid);
    table_write_set->pop_back();
  }
  table_write_set->clear();
  while (!index_write_set->empty()) {
    auto &item = index_write_set->back();
    auto *catalog = item.catalog_;
    // Metadata identifying the table that should be deleted from.
    TableInfo *table_info = catalog->GetTable(item.table_oid_);
    if (item.wtype_ != WType::UPDATE) {
      IndexInfo *index_info = catalog->GetIndex(item.index_oid_);
      if (item.wtype_ == WType::INSERT) {
        index_info->index_->DeleteEntry(item.tuple_, item.rid_, txn);
      } else {
        index_info->index_->InsertEntry(item.tuple_, item.rid_, txn);
      }
    } else {
      table_info->table_->UpdateTupleInPlaceUnsafe({INVALID_TXN_ID, INVALID_TXN_ID, false}, item.tuple_, item.rid_);
    }
    index_write_set->pop_back();
  }
  index_write_set->clear();
  ReleaseLocks(txn);
  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
