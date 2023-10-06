#include <cmath>
#include <memory>
#include <optional>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"
#include "type/value.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  guard = bpm_->FetchPageBasic(header_page->root_page_id_);
  auto root_page = guard.As<BPlusTreePage>();
  return root_page->GetSize() == 0;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  std::optional<ReadPageGuard> target_guard = ReadLookUp(key);
  if (!target_guard.has_value()) {
    return false;
  }
  auto target_page = target_guard->As<LeafPage>();
  auto target_pair = target_page->Find(key, comparator_);
  int index = target_pair.first;
  if (index == -1 || target_pair.second != COMPARE_EQUAL) {
    return false;
  }
  result->push_back(target_page->ValueAt(index));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  std::optional<WritePageGuard> target_guard = WriteLookUp(key);
  if (target_guard.has_value()) {
    auto target_page = target_guard->AsMut<LeafPage>();
    if (target_page->GetSize() < leaf_max_size_ - 1) {
      return target_page->Insert(key, value, comparator_);
    }
    target_guard->Drop();
  }
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  std::unique_ptr<Context> ctx = std::make_unique<Context>();
  if (IsEmpty()) {
    page_id_t root_page_id;
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    auto root_guard = bpm_->NewPageGuarded(&root_page_id);
    auto root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->Insert(key, value, comparator_);
    header_page->root_page_id_ = root_page_id;
    return true;
  }
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  WritePageGuard page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  ctx->header_page_ = std::optional<WritePageGuard>{WritePageGuard(std::move(header_guard))};
  ctx->root_page_id_ = page_guard.PageId();
  auto tree_page = page_guard.AsMut<BPlusTreePage>();
  while (!tree_page->IsLeafPage()) {
    auto internal_page = page_guard.AsMut<InternalPage>();
    auto pair = internal_page->Find(key, comparator_);
    int current_index = GetIndex(pair);
    auto child_page_id = internal_page->ValueAt(current_index);
    ctx->write_set_.emplace_back(std::move(page_guard), current_index);
    page_guard = std::move(bpm_->FetchPageWrite(child_page_id));
    tree_page = page_guard.AsMut<BPlusTreePage>();
    auto safe_size = tree_page->IsLeafPage() ? leaf_max_size_ - 1 : internal_max_size_;
    if (tree_page->GetSize() < safe_size) {  // thread-safe node
      if (ctx->header_page_.has_value()) {
        ctx->header_page_ = std::nullopt;
      }
      ctx->write_set_.clear();
    }
  }
  return InsertLeaf(std::move(page_guard), key, value, ctx.get());
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  std::optional<WritePageGuard> target_guard = WriteLookUp(key);
  if (!target_guard.has_value()) {
    return;
  }
  auto target_page = target_guard->AsMut<LeafPage>();
  if (target_page->GetSize() > target_page->GetMinSize()) {
    return target_page->Remove(key, comparator_);
  }
  target_guard->Drop();
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  if (IsEmpty()) {
    return;
  }
  std::unique_ptr<Context> ctx = std::make_unique<Context>();
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  ctx->header_page_ = std::optional<WritePageGuard>{WritePageGuard(std::move(header_guard))};
  WritePageGuard page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  ctx->root_page_id_ = page_guard.PageId();
  auto tree_page = page_guard.AsMut<BPlusTreePage>();
  while (!tree_page->IsLeafPage()) {
    auto internal_page = page_guard.AsMut<InternalPage>();
    auto pair = internal_page->Find(key, comparator_);
    int current_index = GetIndex(pair);
    auto child_page_id = internal_page->ValueAt(current_index);
    ctx->write_set_.emplace_back(std::move(page_guard), current_index);
    page_guard = std::move(bpm_->FetchPageWrite(child_page_id));
    tree_page = page_guard.AsMut<BPlusTreePage>();
    if (tree_page->GetSize() > tree_page->GetMinSize()) {  // thread-safe node
      if (ctx->header_page_.has_value()) {
        ctx->header_page_ = std::nullopt;
      }
      ctx->write_set_.clear();
    }
  }
  RemoveLeaf(std::move(page_guard), key, ctx.get());
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(std::nullopt, bpm_);
  }
  auto header_page = page_guard.As<BPlusTreeHeaderPage>();
  page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto tree_page = page_guard.As<BPlusTreePage>();
  while (!tree_page->IsLeafPage()) {
    auto internal_page = page_guard.As<InternalPage>();
    page_guard = bpm_->FetchPageRead(internal_page->ValueAt(0));
    tree_page = page_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(std::move(page_guard), bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  int index = 0;
  std::optional<ReadPageGuard> page_guard = ReadLookUp(key);
  if (page_guard.has_value()) {
    auto page = page_guard->As<LeafPage>();
    auto target_pair = page->Find(key, comparator_);
    if (target_pair.second == COMPARE_EQUAL) {
      index = target_pair.first;
    }
  }
  return INDEXITERATOR_TYPE(std::move(page_guard), bpm_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(std::nullopt, bpm_);
  }
  auto header_page = page_guard.As<BPlusTreeHeaderPage>();
  page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto tree_page = page_guard.As<BPlusTreePage>();
  while (!tree_page->IsLeafPage()) {
    auto internal_page = page_guard.As<InternalPage>();
    page_guard = bpm_->FetchPageRead(internal_page->ValueAt(internal_page->GetSize() - 1));
    tree_page = page_guard.As<BPlusTreePage>();
  }
  auto page = page_guard.As<LeafPage>();
  int index = page->GetSize();
  return INDEXITERATOR_TYPE(std::move(page_guard), bpm_, index);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent
    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertLeaf(WritePageGuard guard, const KeyType &key, const ValueType &value, Context *ctx)
    -> bool {
  auto leaf_page = guard.AsMut<LeafPage>();
  if (!leaf_page->Insert(key, value, comparator_)) {
    return false;
  }
  int size = leaf_page->GetSize();
  if (size >= leaf_max_size_) {
    page_id_t new_page_id;
    KeyType new_key = leaf_page->Split(bpm_, &new_page_id);
    if (ctx->IsRootPage(guard.PageId())) {
      return CreateNewRoot(guard.PageId(), new_key, new_page_id, ctx);
    }
    auto parent_guard = std::move(ctx->write_set_.back().first);
    ctx->write_set_.pop_back();
    return InsertInternal(std::move(parent_guard), new_key, RID(new_page_id, 0), ctx);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInternal(WritePageGuard guard, const KeyType &key, const ValueType &value, Context *ctx)
    -> bool {
  auto internal_page = guard.AsMut<InternalPage>();
  page_id_t child_id = value.GetPageId();
  if (!internal_page->Insert(key, child_id, comparator_)) {
    return false;
  }
  int size = internal_page->GetSize();
  if (size > internal_max_size_) {
    page_id_t new_page_id;
    KeyType new_key = internal_page->Split(bpm_, &new_page_id);
    if (ctx->IsRootPage(guard.PageId())) {
      return CreateNewRoot(guard.PageId(), new_key, new_page_id, ctx);
    }
    auto parent_guard = std::move(ctx->write_set_.back().first);
    ctx->write_set_.pop_back();
    return InsertInternal(std::move(parent_guard), new_key, RID(new_page_id, 0), ctx);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveLeaf(WritePageGuard guard, const KeyType &key, Context *ctx) {
  auto leaf_node = guard.AsMut<LeafPage>();
  leaf_node->Remove(key, comparator_);
  int size = leaf_node->GetSize();
  if (ctx->IsRootPage(guard.PageId())) {
    return;
  }
  if (size < leaf_node->GetMinSize()) {
    WritePageGuard parent_page = std::move(ctx->write_set_.back().first);
    auto parent_node = parent_page.AsMut<InternalPage>();
    int curr_index = ctx->write_set_.back().second;
    ctx->write_set_.pop_back();
    int parent_index = 0;
    if (curr_index != 0) {
      parent_index = curr_index;
      WritePageGuard predecessor = bpm_->FetchPageWrite(parent_node->ValueAt(curr_index - 1));
      auto predecessor_node = predecessor.AsMut<LeafPage>();
      int predecessor_size = predecessor_node->GetSize();
      if (size + predecessor_size < leaf_max_size_) {
        predecessor_node->Merge(leaf_node);
        auto parent_key = parent_node->KeyAt(parent_index);
        RemoveInternal(std::move(parent_page), parent_key, ctx);
      } else {
        parent_node->SetKeyAt(parent_index, leaf_node->Redistribute(predecessor_node, false));
      }
    } else {
      parent_index = curr_index + 1;
      WritePageGuard successor = bpm_->FetchPageWrite(parent_node->ValueAt(curr_index + 1));
      auto successor_node = successor.AsMut<LeafPage>();
      int successor_size = successor_node->GetSize();
      if (size + successor_size < leaf_max_size_) {
        leaf_node->Merge(successor_node);
        auto parent_key = parent_node->KeyAt(parent_index);
        RemoveInternal(std::move(parent_page), parent_key, ctx);
      } else {
        parent_node->SetKeyAt(parent_index, leaf_node->Redistribute(successor_node, true));
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInternal(WritePageGuard guard, const KeyType &key, Context *ctx) {
  auto internal_node = guard.AsMut<InternalPage>();
  internal_node->Remove(key, comparator_);
  int size = internal_node->GetSize();
  if (ctx->IsRootPage(guard.PageId())) {
    if (size == 1) {
      page_id_t new_root_page_id = internal_node->ValueAt(0);
      auto header_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = new_root_page_id;
    }
    return;
  }
  if (size < internal_node->GetMinSize()) {
    WritePageGuard parent_page = std::move(ctx->write_set_.back().first);
    auto parent_node = parent_page.AsMut<InternalPage>();
    int curr_index = ctx->write_set_.back().second;
    ctx->write_set_.pop_back();
    int parent_index = 0;
    if (curr_index != 0) {
      parent_index = curr_index;
      auto parent_key = parent_node->KeyAt(parent_index);
      WritePageGuard predecessor = bpm_->FetchPageWrite(parent_node->ValueAt(curr_index - 1));
      auto predecessor_node = predecessor.AsMut<InternalPage>();
      int predecessor_size = predecessor_node->GetSize();
      if (size + predecessor_size <= internal_max_size_) {
        predecessor_node->Merge(internal_node, parent_key);
        RemoveInternal(std::move(parent_page), parent_key, ctx);
      } else {
        parent_node->SetKeyAt(parent_index, internal_node->Redistribute(predecessor_node, parent_key, false));
      }
    } else {
      parent_index = curr_index + 1;
      auto parent_key = parent_node->KeyAt(parent_index);
      WritePageGuard successor = bpm_->FetchPageWrite(parent_node->ValueAt(curr_index + 1));
      auto successor_node = successor.AsMut<InternalPage>();
      int successor_size = successor_node->GetSize();
      if (size + successor_size <= internal_max_size_) {
        internal_node->Merge(successor_node, parent_key);
        RemoveInternal(std::move(parent_page), parent_key, ctx);
      } else {
        parent_node->SetKeyAt(parent_index, internal_node->Redistribute(successor_node, parent_key, true));
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewRoot(page_id_t lchild, const KeyType &key, page_id_t rchild, Context *ctx) -> bool {
  std::pair<KeyType, page_id_t> array[2];
  array[0].second = lchild;
  array[1].first = key;
  array[1].second = rchild;
  page_id_t new_root_page_id;
  auto new_root_guard = bpm_->NewPageGuarded(&new_root_page_id);
  auto new_root_page = new_root_guard.AsMut<InternalPage>();
  new_root_page->Init(array, 0, 2, internal_max_size_);
  auto header_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = new_root_page_id;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetIndex(std::pair<int, int> pair) const -> int {
  int index = pair.first;
  switch (pair.second) {
    case COMPARE_LESS:
      return index - 1;
    case COMPARE_EQUAL:
    case COMPARE_GREATER:
      return index;
    default:
      return -1;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReadLookUp(const KeyType &key) -> std::optional<ReadPageGuard> {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  if (IsEmpty()) {
    return std::nullopt;
  }
  auto header_page = page_guard.As<BPlusTreeHeaderPage>();
  page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto tree_page = page_guard.As<BPlusTreePage>();
  while (!tree_page->IsLeafPage()) {
    auto internal_page = page_guard.As<InternalPage>();
    auto pair = internal_page->Find(key, comparator_);
    int current_index = GetIndex(pair);
    auto child_page_id = internal_page->ValueAt(current_index);
    page_guard = bpm_->FetchPageRead(child_page_id);
    tree_page = page_guard.As<BPlusTreePage>();
  }
  return page_guard;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::WriteLookUp(const KeyType &key) -> std::optional<WritePageGuard> {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  if (IsEmpty()) {
    return std::nullopt;
  }
  auto header_page = page_guard.As<BPlusTreeHeaderPage>();
  auto child_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto child_page_id = header_page->root_page_id_;
  auto tree_page = child_guard.As<BPlusTreePage>();
  while (!tree_page->IsLeafPage()) {
    auto internal_page = child_guard.As<InternalPage>();
    auto pair = internal_page->Find(key, comparator_);
    int current_index = GetIndex(pair);
    child_page_id = internal_page->ValueAt(current_index);
    page_guard = std::move(child_guard);
    child_guard = bpm_->FetchPageRead(child_page_id);
    tree_page = child_guard.As<BPlusTreePage>();
  }
  child_guard.Drop();
  return bpm_->FetchPageWrite(child_page_id);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
