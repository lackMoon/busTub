/**
 * index_iterator.cpp
 */
#include <optional>
#include <utility>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::optional<ReadPageGuard> guard, BufferPoolManager *bpm, int index)
    : guard_(std::move(guard)),
      bpm_(bpm),
      page_id_(guard_.has_value() ? guard_->PageId() : INVALID_PAGE_ID),
      index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { guard_ = std::nullopt; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (!guard_.has_value()) {
    return true;
  }
  auto page = guard_->As<LeafPage>();
  return page->GetNextPageId() == INVALID_PAGE_ID && index_ == page->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (!guard_.has_value()) {
    throw Exception(ExceptionType::INVALID, "INDEX ITERATOR: can not de-reference a empty iterator");
  }
  auto page = guard_->As<LeafPage>();
  return page->At(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (!guard_.has_value()) {
    throw Exception(ExceptionType::INVALID, "INDEX ITERATOR: can not operate a empty iterator");
  }
  auto page = guard_->As<LeafPage>();
  auto next_page_id = page->GetNextPageId();
  index_++;
  if (index_ == page->GetSize() && next_page_id != INVALID_PAGE_ID) {
    guard_ = std::optional<ReadPageGuard>(std::move(bpm_->FetchPageRead(page->GetNextPageId())));
    index_ = 0;
    page_id_ = next_page_id;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
