//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstring>
#include <memory>
#include <optional>
#include <utility>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size),
      disk_manager_(disk_manager),
      /*disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), */ log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::shared_mutex> latch(latch_);
  page_id_t new_page_id = AllocatePage();
  *page_id = new_page_id;
  return CreatePage(new_page_id, AccessType::Unknown);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "BUFFER POOL MANAGER: Invalid Page Request");
  std::unique_lock<std::shared_mutex> latch(latch_);
  Page *fetch_page;
  frame_id_t frame_id;
  if (page_table_.count(page_id) == 1) {
    frame_id = page_table_.at(page_id);
    fetch_page = &pages_[frame_id];
    AccessPage(fetch_page, frame_id, access_type);
  } else {
    fetch_page = CreatePage(page_id, access_type);
    if (fetch_page == nullptr) {
      return nullptr;
    }
    // auto read_promise = disk_scheduler_->CreatePromise();
    // auto read_future = read_promise.get_future();
    // auto request = std::make_unique<DiskRequest>(false, fetch_page->GetData(), page_id, std::move(read_promise));
    // disk_scheduler_->Schedule(std::move(request));
    // read_future.get();
    disk_manager_->ReadPage(page_id, fetch_page->GetData());
  }
  return fetch_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::shared_lock<std::shared_mutex> latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  auto frame_id = page_table_.at(page_id);
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() == 0) {
    return false;
  }
  page->pin_count_--;
  page->is_dirty_ |= is_dirty;
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::shared_lock<std::shared_mutex> latch(latch_);
  if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0) {
    return false;
  }
  auto frame_id = page_table_.at(page_id);
  Page *page = &pages_[frame_id];
  // auto write_promise = disk_scheduler_->CreatePromise();
  // auto write_future = write_promise.get_future();
  // auto request = std::make_unique<DiskRequest>(true, page->GetData(), page_id, std::move(write_promise));
  // disk_scheduler_->Schedule(std::move(request));
  // write_future.get();
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &entry : page_table_) {
    FlushPage(entry.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::shared_mutex> latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  auto frame_id = page_table_.at(page_id);
  Page *page = &pages_[page_table_.at(page_id)];
  if (page->GetPinCount() != 0) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  page->is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::EXECUTION,
                    "BUFFER POOL MANAGER: error occured in FetchPageRead -- No available Page");
  }
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::EXECUTION,
                    "BUFFER POOL MANAGER: error occured in FetchPageWrite -- No available Page");
  }
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

void BufferPoolManager::AccessPage(Page *page, frame_id_t frame_id, AccessType access_type) {
  page->pin_count_++;
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
}

auto BufferPoolManager::CreatePage(page_id_t page_id, AccessType access_type) -> Page * {
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  Page *page = &pages_[frame_id];
  page_id_t old_page_id = page->GetPageId();
  page_table_.erase(old_page_id);
  page_table_[page_id] = frame_id;
  if (page->IsDirty()) {
    // auto write_promise = disk_scheduler_->CreatePromise();
    // auto write_future = write_promise.get_future();
    // auto request = std::make_unique<DiskRequest>(true, page->GetData(), old_page_id, std::move(write_promise));
    // disk_scheduler_->Schedule(std::move(request));
    // write_future.get();
    disk_manager_->WritePage(old_page_id, page->GetData());
    page->is_dirty_ = false;
  }
  page->ResetMemory();
  page->page_id_ = page_id;
  AccessPage(page, frame_id, access_type);
  return page;
}
}  // namespace bustub
