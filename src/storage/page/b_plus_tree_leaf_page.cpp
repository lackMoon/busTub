//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_leaf_page.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(MappingType *array, int start, int end, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(end - start);
  SetMaxSize(max_size);
  std::copy(array + start, array + end, array_);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

/*
 * Helper method to find and return the value associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BufferPoolManager *bpm, page_id_t *page_id) -> KeyType {
  int size = GetSize();
  auto new_page_guard = bpm->NewPageGuarded(page_id);
  auto new_page = new_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  new_page->Init(array_, size / 2, size, GetMaxSize());
  this->SetNextPageId(*page_id);
  SetSize(size / 2);
  return new_page->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType &key, KeyComparator &comparator) const -> std::pair<int, int> {
  int left = 0;
  int right = GetSize() - 1;
  int flg = 0;
  int mid = -1;
  while (left <= right) {
    mid = (left + right) >> 1;
    int result = comparator(key, array_[mid].first);
    if (result < 0) {
      right = mid - 1;
      flg = COMPARE_LESS;
    } else if (result > 0) {
      left = mid + 1;
      flg = COMPARE_GREATER;
    } else {
      flg = COMPARE_EQUAL;
      break;
    }
  }
  return std::make_pair(mid, flg);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator) -> bool {
  int size = GetSize();
  std::pair<int, int> target_pair = Find(key, comparator);
  int index = target_pair.first;
  int flg = target_pair.second;
  if (index == -1) {  // Empty
    array_[0] = MappingType(key, value);
  } else if (flg == COMPARE_EQUAL) {  // Duplicate Key
    return false;
  } else if (flg == COMPARE_GREATER && index > size) {  // Back Insert
    array_[++index] = MappingType(key, value);
  } else {
    index = (flg == COMPARE_LESS) ? index : index + 1;
    for (int i = size; i > index; i--) {
      array_[i] = array_[i - 1];
    }
    array_[index] = MappingType(key, value);
  }
  IncreaseSize(1);
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
