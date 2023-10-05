//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_internal_page.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(MappingType *array, int start, int end, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(end - start);
  SetMaxSize(max_size);
  array_[0].second = array[start].second;
  Copy(array, start + 1, end, 1);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index == 0) {
    throw Exception(ExceptionType::INVALID, "B+ Tree: the index of the key in internal page must be non-zero");
  }
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::All() const -> const MappingType * { return array_; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BufferPoolManager *bpm, page_id_t *page_id) -> KeyType {
  int size = GetSize();
  auto new_page_guard = bpm->NewPageGuarded(page_id);
  auto new_page = new_page_guard.AsMut<B_PLUS_TREE_INTERNAL_PAGE_TYPE>();
  KeyType new_key = array_[size / 2].first;
  new_page->Init(array_, size / 2, size, GetMaxSize());
  SetSize(size / 2);
  return new_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Find(KeyType key, KeyComparator &comparator) const -> std::pair<int, int> {
  int left = 1;
  int right = GetSize() - 1;
  int mid = -1;
  int flg = 0;
  int result;
  if ((result = comparator(key, array_[left].first)) <= 0) {
    flg = result < 0 ? COMPARE_LESS : COMPARE_EQUAL;
    return std::make_pair(left, flg);
  }
  if ((result = comparator(key, array_[right].first)) >= 0) {
    flg = result > 0 ? COMPARE_GREATER : COMPARE_EQUAL;
    return std::make_pair(right, flg);
  }
  while (left <= right) {
    mid = (left + right) >> 1;
    result = comparator(key, array_[mid].first);
    if (result < 0) {
      flg = COMPARE_LESS;
      right = mid - 1;
    } else if (result > 0) {
      flg = COMPARE_GREATER;
      left = mid + 1;
    } else {
      flg = COMPARE_EQUAL;
      break;
    }
  }
  return std::make_pair(mid, flg);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator)
    -> bool {
  int size = GetSize();
  std::pair<int, int> target_pair = Find(key, comparator);
  int index = target_pair.first;
  int flg = target_pair.second;
  if (flg == COMPARE_EQUAL) {  // Duplicate Key
    return false;
  }
  if (flg == COMPARE_GREATER && index >= size - 1) {  // Back Insert
    array_[++index] = MappingType(key, value);
  } else {
    index = (flg == COMPARE_LESS) ? index : index + 1;
    for (int i = size; i > index; i--) {
      array_[i] = array_[i - 1];
    }
    if (index == 0) {
      array_[0].second = value;
      array_[1].first = key;
    } else {
      array_[index] = MappingType(key, value);
    }
  }
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertHead(const KeyType &key, const ValueType &value) {
  int size = GetSize();
  for (int i = size; i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  array_[1].first = key;
  array_[0].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, KeyComparator &comparator) {
  std::pair<int, int> target_pair = Find(key, comparator);
  int index = target_pair.first;
  int flg = target_pair.second;
  if (index == -1 || flg != COMPARE_EQUAL) {  // No target key
    return;
  }
  Remove(index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  if (index < size - 1) {
    if (index == 0) {
      array_[0].second = array_[1].second;
      index++;
    }
    for (int i = index; i < size; i++) {
      array_[i] = array_[i + 1];
    }
  }
  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
