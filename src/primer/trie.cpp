#include "primer/trie.h"
#include <cstddef>
#include <map>
#include <memory>
#include <stack>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (root_ == nullptr) {
    return nullptr;
  }
  if (key.empty()) {
    key = std::string_view("\"\"");
  }
  auto curr_ptr = root_;
  for (const char &k : key) {
    if (!(curr_ptr->children_.count(k))) {
      return nullptr;
    }
    curr_ptr = curr_ptr->children_.at(k);
  }
  auto node = dynamic_cast<const TrieNodeWithValue<T> *>(curr_ptr.get());
  return node == nullptr ? nullptr : node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  auto new_trie = root_ == nullptr ? std::make_unique<TrieNode>() : root_->Clone();
  if (key.empty()) {
    key = std::string_view("\"\"");
  }
  size_t length = key.size();
  std::shared_ptr<T> node_value = std::make_shared<T>(std::move(value));
  std::stack<std::pair<char, std::shared_ptr<const TrieNode>>> stack;
  auto curr_ptr = root_ == nullptr ? std::make_shared<const TrieNode>() : root_;
  for (size_t index = 0; index < length; index++) {
    const char &k = key[index];
    std::shared_ptr<const TrieNode> child;
    if (curr_ptr->children_.count(k)) {
      child = index == length - 1
                  ? std::make_shared<const TrieNodeWithValue<T>>(curr_ptr->children_.at(k)->children_, node_value)
                  : curr_ptr->children_.at(k);
    } else {
      child = index == length - 1 ? std::make_shared<const TrieNodeWithValue<T>>(node_value)
                                  : std::make_shared<const TrieNode>();
    }
    stack.push({k, child});
    curr_ptr = child;
  }
  auto curr_pair = stack.top();
  auto curr_key = curr_pair.first;
  auto curr_node = curr_pair.second;
  stack.pop();
  while (!stack.empty()) {
    auto new_pair = stack.top();
    auto new_node = new_pair.second->Clone();
    new_node->children_[curr_key] = curr_node;
    curr_key = new_pair.first;
    curr_node = std::shared_ptr<const TrieNode>(std::move(new_node));
    stack.pop();
  }
  new_trie->children_[curr_key] = curr_node;
  return Trie(std::shared_ptr<const TrieNode>(std::move(new_trie)));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (root_ == nullptr) {
    return Trie(std::make_shared<const TrieNode>());
  }
  auto new_trie = root_->Clone();
  size_t length = key.size();
  std::stack<std::pair<char, std::shared_ptr<const TrieNode>>> stack;
  std::unique_ptr<TrieNode> remove_node;
  auto curr_ptr = root_;
  for (size_t index = 0; index < length; index++) {
    const char &k = key[index];
    if (curr_ptr->children_.count(k) == 0) {
      return Trie(root_);
    }
    if (index == length - 1) {
      auto target_node = curr_ptr->children_.at(k);
      if (target_node->children_.empty()) {  // If a node doesn't have children any more, you should remove it.
        remove_node = nullptr;
      } else if (target_node->is_value_node_) {  // If the node doesn't contain a value any more, you should convert it
                                                 // to `TrieNode`.
        remove_node = target_node->TrieNode::Clone();
      } else {
        return Trie(root_);
      }
      curr_ptr = std::shared_ptr<const TrieNode>(std::move(remove_node));
    } else {
      curr_ptr = curr_ptr->children_.at(k);
    }
    stack.push({k, curr_ptr});
  }
  auto curr_pair = stack.top();
  auto curr_key = curr_pair.first;
  auto curr_node = curr_pair.second;
  stack.pop();
  while (!stack.empty()) {
    auto new_pair = stack.top();
    auto new_node = new_pair.second->Clone();
    if (curr_node == nullptr) {
      auto iter = new_node->children_.find(curr_key);
      new_node->children_.erase(iter);
    } else {
      new_node->children_[curr_key] = curr_node;
    }
    curr_key = new_pair.first;
    curr_node = std::shared_ptr<const TrieNode>(std::move(new_node));
    stack.pop();
  }
  new_trie->children_[curr_key] = curr_node;
  return Trie(std::shared_ptr<const TrieNode>(std::move(new_trie)));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
