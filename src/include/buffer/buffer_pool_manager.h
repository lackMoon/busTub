//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstring>
#include <deque>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <queue>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class DiskRequest {
 public:
  DiskRequest(bool is_write, char *data, page_id_t page_id, std::promise<bool> call_back)
      : is_write_(is_write), page_id_(page_id), callback_(std::move(call_back)) {
    if (is_write_) {
      data_ = new char[BUSTUB_PAGE_SIZE];
      memmove(data_, data, BUSTUB_PAGE_SIZE);
    } else {
      data_ = data;
    }
  }

  ~DiskRequest() {
    if (data_ != nullptr && is_write_) {
      memset(data_, 0, BUSTUB_PAGE_SIZE);
      delete[] data_;
      data_ = nullptr;
    }
  }

  DiskRequest(DiskRequest &&request) noexcept {
    is_write_ = request.is_write_;
    page_id_ = request.page_id_;
    callback_ = std::move(request.callback_);
    if (is_write_) {
      memmove(data_, request.data_, BUSTUB_PAGE_SIZE);
      memset(request.data_, 0, BUSTUB_PAGE_SIZE);
      delete[] request.data_;
      request.data_ = nullptr;
    } else {
      data_ = request.data_;
    }
  }
  /** Flag indicating whether the request is a write or a read. */
  bool is_write_;

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  char *data_;

  /** ID of the page being read from / written to disk. */
  page_id_t page_id_;

  /** Callback used to signal to the request issuer when the request has been completed. */
  std::promise<bool> callback_;
};

class RequestChannel {
 public:
  RequestChannel() { cached_.reserve(1024); }
  ~RequestChannel() {
    for (auto item : cached_) {
      auto data = item.second.second;
      delete []data;
      data = nullptr;
    }
  };

  /**
   * @brief Inserts an element into a shared queue.
   *
   * @param element The element to be inserted.
   */
  void Put(std::optional<std::unique_ptr<DiskRequest>> request) {
    std::unique_lock<std::mutex> lk(m_);
    auto page_id = request->get()->page_id_;
    if (cached_.count(page_id) != 0) {
      cached_.at(page_id).first = false;
    }
    q_.push(std::move(request));
    lk.unlock();
    cv_.notify_all();
  }

  /**
   * @brief Gets an element from the shared queue. If the queue is empty, blocks until an element is available.
   */
  auto Get() -> std::optional<std::unique_ptr<DiskRequest>> {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [&]() { return !q_.empty(); });
    auto r = std::move(q_.front());
    q_.pop();
    return r;
  }

  auto Cached(std::unique_ptr<DiskRequest> &request) -> bool {
    std::unique_lock<std::mutex> lk(m_);
    auto page_id = request->page_id_;
    if (cached_.count(page_id) != 0 && cached_.at(page_id).first) {
      auto data = cached_.at(page_id).second;
      memcpy(request->data_, data, BUSTUB_PAGE_SIZE);
      return true;
    }
    return false;
  }

  void SetCache(page_id_t page_id, char *data) {
    std::unique_lock<std::mutex> lk(m_);
    if (cached_.count(page_id) == 0) {
      cached_.insert({page_id, {true, new char[BUSTUB_PAGE_SIZE]}});
    }
    cached_[page_id].first = true;
    memcpy(cached_[page_id].second, data, BUSTUB_PAGE_SIZE);
  }

 private:
  std::mutex m_;
  std::condition_variable cv_;
  std::queue<std::optional<std::unique_ptr<DiskRequest>>> q_;
  std::unordered_map<page_id_t, std::pair<bool, char*>> cached_;
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
    // Spawn the background thread
    background_thread_.emplace([&] { StartWorkerThread(); });
  }
  ~DiskScheduler() {
    request_queue_.Put(std::nullopt);
    if (background_thread_.has_value()) {
      background_thread_->join();
    }
  }

  /**
   *
   * @brief Schedules a request for the DiskManager to execute.
   *
   * @param r The request to be scheduled.
   */
  void Schedule(std::unique_ptr<DiskRequest> r) {
    if (!r->is_write_) {
      if (!request_queue_.Cached(r)) {
        disk_manager_->ReadPage(r->page_id_, r->data_);
        request_queue_.SetCache(r->page_id_, r->data_);
      }
      r->callback_.set_value(true);
    } else {
      r->callback_.set_value(true);
      request_queue_.Put(std::move(r));
    }
  }

  /**
   *
   * @brief Background worker thread function that processes scheduled requests.
   *
   * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
   * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
   */
  void StartWorkerThread() {
    auto r = request_queue_.Get();
    while (r != std::nullopt) {
      auto &request = r.value();
      disk_manager_->WritePage(request->page_id_, request->data_);
      r = request_queue_.Get();
    }
  }

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

 private:
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_;
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  RequestChannel request_queue_;
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  std::optional<std::thread> background_thread_;
};

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManager {
 public:
  /**
   * @brief Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the LookBack constant k for the LRU-K replacer
   * @param log_manager the log manager (for testing only: nullptr = disable logging). Please ignore this for P1.
   */
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);

  /**
   * @brief Destroy an existing BufferPoolManager.
   */
  ~BufferPoolManager();

  /** @brief Return the size (number of frames) of the buffer pool. */
  auto GetPoolSize() -> size_t { return pool_size_; }

  /** @brief Return the pointer to all the pages in the buffer pool. */
  auto GetPages() -> Page * { return pages_; }

  /**
   * TODO(P1): Add implementation
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  auto NewPage(page_id_t *page_id) -> Page *;

  /**
   * TODO(P1): Add implementation
   *
   * @brief PageGuard wrapper for NewPage
   *
   * Functionality should be the same as NewPage, except that
   * instead of returning a pointer to a page, you return a
   * BasicPageGuard structure.
   *
   * @param[out] page_id, the id of the new page
   * @return BasicPageGuard holding a new page
   */
  auto NewPageGuarded(page_id_t *page_id) -> BasicPageGuard;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
   * but all frames are currently in use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
   * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
   * and replace the old page in the frame. Similar to NewPage(), if the old page is dirty, you need to write it back
   * to disk and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
   *
   * @param page_id id of page to be fetched
   * @param access_type type of access to the page, only needed for leaderboard tests.
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
   */
  auto FetchPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> Page *;

  /**
   * TODO(P1): Add implementation
   *
   * @brief PageGuard wrappers for FetchPage
   *
   * Functionality should be the same as FetchPage, except
   * that, depending on the function called, a guard is returned.
   * If FetchPageRead or FetchPageWrite is called, it is expected that
   * the returned page already has a read or write latch held, respectively.
   *
   * @param page_id, the id of the page to fetch
   * @return PageGuard holding the fetched page
   */
  auto FetchPageBasic(page_id_t page_id) -> BasicPageGuard;
  auto FetchPageRead(page_id_t page_id) -> ReadPageGuard;
  auto FetchPageWrite(page_id_t page_id) -> WritePageGuard;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @param access_type type of access to the page, only needed for leaderboard tests.
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   */
  auto UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type = AccessType::Unknown) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush the target page to disk.
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
   * Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  auto FlushPage(page_id_t page_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush all the pages in the buffer pool to disk.
   */
  void FlushAllPages();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   * page is pinned and cannot be deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  auto DeletePage(page_id_t page_id) -> bool;

 private:
  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** The next page id to be allocated  */
  std::atomic<page_id_t> next_page_id_ = 0;

  /** Array of buffer pool pages. */
  Page *pages_;

  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));

  /** Pointer to the disk sheduler. */
  std::unique_ptr<DiskScheduler> disk_scheduler_;
  /** Pointer to the log manager. Please ignore this for P1. */
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  /** Replacer to find unpinned pages for replacement. */
  std::unique_ptr<LRUKReplacer> replacer_;
  /** List of free frames that don't have any pages on them. */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::shared_mutex latch_;

  /**
   * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
   * @return the id of the allocated page
   */
  auto AllocatePage() -> page_id_t;

  /**
   * @brief Deallocate a page on disk. Caller should acquire the latch before calling this function.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }

  void AccessPage(Page *page, frame_id_t frame_id, AccessType access_type);

  auto CreatePage(page_id_t page_id, AccessType access_type) -> Page *;
};
}  // namespace bustub
