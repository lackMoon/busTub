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
  std::copy(array + start + 1, array + end, array_ + 1);
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
  std::pair<int, int> target_pair = Find(key, comparator);
  int index = target_pair.first;
  int flg = target_pair.second;
  if (flg == COMPARE_EQUAL) {  // Duplicate Key
    return false;
  }
  index = (flg == COMPARE_LESS) ? index : index + 1;
  Insert(index, key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(int index, const KeyType &key, const ValueType &value) {
  int size = GetSize();
  if (index >= size) {  // Back Insert
    array_[index] = MappingType(key, value);
  } else {
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
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(B_PLUS_TREE_INTERNAL_PAGE_TYPE *node, const KeyType &key) {
  int size = GetSize();
  int node_size = node->GetSize();
  auto array = node->All();
  std::copy(array, array + node_size, array_ + size);
  SetKeyAt(size, key);
  IncreaseSize(node_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Redistribute(B_PLUS_TREE_INTERNAL_PAGE_TYPE *node, const KeyType &parent_key,
                                                  bool is_predecessor) -> KeyType {
  int size = GetSize();
  int node_size = node->GetSize();
  KeyType key;
  ValueType value;
  if (is_predecessor) {
    key = node->KeyAt(1);
    value = node->ValueAt(0);
    node->Remove(0);
    Insert(size, parent_key, value);
  } else {
    key = node->KeyAt(node_size - 1);
    value = node->ValueAt(node_size - 1);
    node->Remove(node_size - 1);
    Insert(0, parent_key, value);
  }
  IncreaseSize(1);
  return key;
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
