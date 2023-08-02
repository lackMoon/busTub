#include "primer/trie_store.h"
#include <optional>
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  std::unique_lock<std::mutex> root_lock(root_lock_);
  auto root = root_;
  root_lock.unlock();
  auto value = root.Get<T>(key);
  return value == nullptr ? std::nullopt : std::optional<ValueGuard<T>>{ValueGuard(root, *value)};
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  std::unique_lock<std::mutex> write_lock(write_lock_);
  auto root = root_.Put(key, std::move(value));
  std::unique_lock<std::mutex> root_lock(root_lock_);
  root_ = std::move(root);
  root_lock.unlock();
}

void TrieStore::Remove(std::string_view key) {
  std::unique_lock<std::mutex> write_lock(write_lock_);
  auto root = root_.Remove(key);
  std::unique_lock<std::mutex> root_lock(root_lock_);
  root_ = std::move(root);
  root_lock.unlock();
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
