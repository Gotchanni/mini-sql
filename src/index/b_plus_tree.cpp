#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
        Page *header_page_obj = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
        if (header_page_obj == nullptr) {
            return;
        }
        IndexRootsPage *header_page = reinterpret_cast<IndexRootsPage *>(header_page_obj->GetData());
        page_id_t temp_root_page_id;
        if(!header_page->GetRootId(index_id_,&temp_root_page_id)){
          root_page_id_ = INVALID_PAGE_ID;
          UpdateRootPageId(1);
        }
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,false);
        
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  // 情况1：初始调用，current_page_id 通常为 INVALID_PAGE_ID
  if (current_page_id == INVALID_PAGE_ID) {
    // 这是销毁整个树的入口。
    // 我们需要从一个特殊的地方（IndexRootsPage）获取这棵B+树真正的根页面ID。
    Page *header_page_obj = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    if (header_page_obj == nullptr) {
      // 如果无法获取存储根页面ID的"头部页面"，说明可能存在更深层的问题，
      // 或者树的相关信息已丢失，此时无法继续，直接返回。
      return;
    }
    // 将头部页面的原始数据转换为 IndexRootsPage 类型，方便我们调用其方法。
    IndexRootsPage *header_page = reinterpret_cast<IndexRootsPage *>(header_page_obj->GetData());
    
    page_id_t actual_root_page_id;
    // 尝试从头部页面中根据当前树的 index_id_ 获取其根页面ID。
    if (header_page->GetRootId(index_id_, &actual_root_page_id)) {
      // 成功获取到根页面ID后，我们不再需要头部页面了，所以要 Unpin它。
      buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false); 
      if (actual_root_page_id != INVALID_PAGE_ID) {
        // 如果获取到的根页面ID是有效的，就以这个ID为起点，开始递归销毁过程。
        Destroy(actual_root_page_id); 
        
        // 根据具体需求，这里可能需要重新获取 header_page 并调用 header_page->Delete(index_id_)
        // 或者将其 root_id 更新为 INVALID_PAGE_ID。
      }
    } else {
      // 如果在头部页面中找不到这个 index_id_ 对应的根页面ID，
      // 可能意味着树已经被销毁或者为空。同样 Unpin 头部页面。
      buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
    }
    // 处理完初始 INVALID_PAGE_ID 的情况后，函数返回。
    return; 
  }

  // 情况2：递归调用，current_page_id 是一个有效的页面ID
  // 获取当前要处理的页面。
  Page *page_obj = buffer_pool_manager_->FetchPage(current_page_id);
  if (page_obj == nullptr) {
    // 如果页面无法获取（例如，它可能已经被其他操作删除了，或者缓冲池管理器出错），
    // 我们无法处理它，直接返回。
    return;
  }

  // 将获取到的页面的原始数据转换为 BPlusTreePage 类型。
  // BPlusTreePage 是叶子页面和内部页面的基类，它有一个 IsLeafPage() 方法。
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page_obj->GetData());

  if (node->IsLeafPage()) {
    // 如果当前节点是叶子页面：
    // 叶子页面没有子节点需要进一步递归销毁。
    // 我们只需要 Unpin（解除锁定）然后从缓冲池管理器中删除这个页面。
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    buffer_pool_manager_->DeletePage(current_page_id);
  } else {
    // 如果当前节点是内部页面：
    // 首先，将节点转换为更具体的 InternalPage 类型，以便访问其子节点。
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    // 遍历这个内部节点中存储的所有子页面的ID。
    for (int i = 0; i < internal_node->GetSize(); ++i) {
      // 对于每一个子页面的ID (internal_node->ValueAt(i))，递归调用 Destroy 函数。
      // 这会确保在删除父节点之前，其所有子节点都已经被处理和删除了。
      Destroy(internal_node->ValueAt(i));
    }
    // 当这个内部节点的所有子节点都被销毁后，
    // 我们 Unpin 并删除当前这个内部页面。
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    buffer_pool_manager_->DeletePage(current_page_id);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  // Step 2 & 3: 如果树为空，返回 false
  if (IsEmpty()) {
    return false;
  }

  // Step 4: 获取 root_page_id_ 对应的页面
  page_id_t current_page_id = root_page_id_;
  Page *current_page_obj = buffer_pool_manager_->FetchPage(current_page_id);
  if (current_page_obj == nullptr) {
    // 无法获取根页面
    return false;
  }

  // Step 5: 将页面数据转换为 BPlusTreePage 类型
  BPlusTreePage *current_node_ptr = reinterpret_cast<BPlusTreePage *>(current_page_obj->GetData());

  // Step 6: 当 node 不是叶子页面时 (traverse to leaf)
  while (!current_node_ptr->IsLeafPage()) {
    // Step 7: 将 node 转换为 InternalPage 类型
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(current_node_ptr);
    
    // Step 8: 获取要查找的子页面 ID
    // InternalPage::Lookup (或类似方法) 应使用 key 和 processor_ 来找到正确的子页面ID
    page_id_t child_page_id = internal_node->Lookup(key, processor_);
    
    // Step 9: 释放当前 node 页面
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    
    // Step 10: 获取子页面
    current_page_id = child_page_id;
    current_page_obj = buffer_pool_manager_->FetchPage(current_page_id);
    if (current_page_obj == nullptr) {
      // 无法获取子页面
      return false; 
    }
    
    // Step 11: 将子页面数据转换为 BPlusTreePage 类型
    current_node_ptr = reinterpret_cast<BPlusTreePage *>(current_page_obj->GetData());
  }

  // 此时, current_node_ptr 指向一个叶子页面 (current_page_obj 也指向它)
  // Step 12: 将 node 转换为 LeafPage 类型
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(current_node_ptr);

  // Step 13 & 14 & 15: 查找 key 对应的值, 如果找到值，将值添加到 result 中
  RowId temp_row_id; // Declare a temporary RowId to store the lookup result
  bool found = leaf_node->Lookup(key, temp_row_id, processor_); // Pass temp_row_id

  if (found) {
    result.clear(); // Ensure result vector is clean for the single value
    result.push_back(temp_row_id); // Add the found RowId to the result vector
  }

  // Step 16: 释放 node 页面 (叶子页面)
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  
  // Step 17: 返回是否找到值
  return found;
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
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) { 
  if(IsEmpty()){
    StartNewTree(key,value);
    return true;
  }
  return InsertIntoLeaf(key,value,transaction);
 }
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  Page *page_obj = buffer_pool_manager_->NewPage(root_page_id_);
  if(page_obj == nullptr){
    throw std::exception();
  }
  LeafPage *root_page = reinterpret_cast<LeafPage *>(page_obj->GetData());
  root_page->Init(INVALID_PAGE_ID,leaf_max_size_);
  root_page->Insert(key,value,processor_);
  buffer_pool_manager_->UnpinPage(root_page_id_,true);
  UpdateRootPageId(0);

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) { 
  // 步骤 1 & 2: 找到正确的叶子页面并进行类型转换 
  page_id_t current_page_id = root_page_id_;
  Page *current_page_obj = buffer_pool_manager_->FetchPage(current_page_id);

  if (current_page_obj == nullptr) {
    // 如果树非空（IsEmpty()检查后调用此函数），则 root_page_id_ 应该是有效的。
    // 此处获取根页面失败是很严重的错误。
    // LOG(ERROR) << "InsertIntoLeaf: Failed to fetch root page " << root_page_id_;
    throw std::runtime_error("InsertIntoLeaf: Failed to fetch root page.");
  }

  BPlusTreePage *current_bpt_page = reinterpret_cast<BPlusTreePage *>(current_page_obj->GetData());

  // 遍历B+树找到目标叶子节点
  while (!current_bpt_page->IsLeafPage()) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(current_bpt_page);
    page_id_t child_page_id = internal_page->Lookup(key, processor_);
    
    buffer_pool_manager_->UnpinPage(current_page_id, false); // 解锁当前内部节点
    
    current_page_id = child_page_id;
    current_page_obj = buffer_pool_manager_->FetchPage(current_page_id);
    if (current_page_obj == nullptr) {
      // LOG(ERROR) << "InsertIntoLeaf: Failed to fetch page " << current_page_id << " during traversal.";
      throw std::runtime_error("InsertIntoLeaf: Failed to fetch page during B+ tree traversal.");
    }
    current_bpt_page = reinterpret_cast<BPlusTreePage *>(current_page_obj->GetData());
  }

  // 此刻, current_page_obj 指向目标叶子页面, 并且已被锁定 (pinned)
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(current_bpt_page);
  page_id_t leaf_page_id = current_page_id; // 保存页面ID用于后续解锁

  // 步骤 3: 检查键是否存在
  RowId temp_val; // Lookup 需要一个 RowId 参数, 但此处我们不关心其值
  if (leaf_node->Lookup(key, temp_val, processor_)) {
    // 键已存在，不允许插入重复键
    std::cout << "Duplicate key" << std::endl; // 
    buffer_pool_manager_->UnpinPage(leaf_page_id, false); // 释放 leaf_node 页面
    return false; // 重复键返回 false
  }

  // 步骤 4: 如果叶子页面未满，则插入键和值
  if (leaf_node->GetSize() < leaf_node->GetMaxSize()) { 
    leaf_node->Insert(key, value, processor_);
    buffer_pool_manager_->UnpinPage(leaf_page_id, true); // 释放 leaf_node 页面 (标记为脏页)
    return true;
  }

  // 步骤 5: 否则 (叶子页面已满)，分裂叶子页面 
  LeafPage *new_leaf_node = Split(leaf_node, transaction); // Split() 返回新的 (右兄弟) 叶子页面。
                                                          // Split 应该处理新页面的分配、锁定、数据迁移，并返回锁定的 new_leaf_node。
                                                          // leaf_node (原始页面) 也被修改并保持锁定状态。
  if (new_leaf_node == nullptr) {
      buffer_pool_manager_->UnpinPage(leaf_page_id, false); // 解锁原始叶子页面
      return false; // 或者抛出异常
  }
  
  // 需要提升到父节点的键是新右兄弟节点的第一个键。
  GenericKey *promoted_key = new_leaf_node->KeyAt(0);


  // 分裂后，原始的 key/value 需要插入到旧的 leaf_node 或新的 new_leaf_node 中。
  if (processor_.CompareKeys(key, promoted_key) < 0) {
    // 要插入的键小于新页面的第一个键，因此它属于旧的 (左) 页面。
    leaf_node->Insert(key, value, processor_);
  } else {
    // 要插入的键大于或等于新页面的第一个键，因此它属于新的 (右) 页面。
    new_leaf_node->Insert(key, value, processor_);
  }
  
  InsertIntoParent(leaf_node, promoted_key, new_leaf_node, transaction);
  

  buffer_pool_manager_->UnpinPage(leaf_page_id, true);             // 解锁原始叶子页面 (现在是左孩子)，标记为脏页。
  buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true); // 解锁新叶子页面 (现在是右孩子)，标记为脏页。
  
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id; 
  Page *page_obj = buffer_pool_manager_->NewPage(new_page_id);
  if(page_obj == nullptr){
    throw std::exception();
  }
  InternalPage *new_internal_node = reinterpret_cast<InternalPage *>(page_obj->GetData());
  new_internal_node->Init(INVALID_PAGE_ID,node->GetParentPageId(),node->GetKeySize(),node->GetMaxSize());
  node->MoveHalfTo(new_internal_node,buffer_pool_manager_);
  return new_internal_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) { 
  page_id_t new_page_id; 
  Page *page_obj = buffer_pool_manager_->NewPage(new_page_id);
  if(page_obj == nullptr){
    throw std::exception();
  }
  LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(page_obj->GetData());
  new_leaf_node->Init(INVALID_PAGE_ID,node->GetParentPageId(),node->GetKeySize(),node->GetMaxSize());
  node->MoveHalfTo(new_leaf_node);
  return new_leaf_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if(old_node->IsRootPage()){
    Page *page_obj = buffer_pool_manager_->NewPage(root_page_id_);
    if(page_obj == nullptr){
      throw std::exception();
    }
    InternalPage *new_root = reinterpret_cast<InternalPage *>(page_obj->GetData());
    new_root->Init(INVALID_PAGE_ID,INVALID_PAGE_ID,old_node->GetKeySize(),old_node->GetMaxSize());
    new_root->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(),true);
    return;
  }
  Page *page_obj = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  if(page_obj == nullptr){
    throw std::exception();
  }
  InternalPage *parent = reinterpret_cast<InternalPage *>(page_obj->GetData());
  parent->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
  if(parent->GetSize() > parent->GetMaxSize()){
    BPlusTreeInternalPage *new_parent = Split(parent,transaction);
    GenericKey *promoted_key = new_parent->KeyAt(0);    // 使用新父节点的第一个键作为提升键
    InsertIntoParent(parent, promoted_key, new_parent, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
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
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) {
    return;
  }
  Page *leaf_page_obj = FindLeafPage(key, root_page_id_, false);
  if (leaf_page_obj == nullptr) {
    return;
  }
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(leaf_page_obj->GetData());
  page_id_t current_leaf_page_id = leaf_page->GetPageId(); // 保存页面ID，用于后续unpin

  RowId temp_row_id; // Lookup 需要一个 RowId 参数, 但此处我们不关心其值
  if (!leaf_page->Lookup(key, temp_row_id, processor_)) {
    buffer_pool_manager_->UnpinPage(current_leaf_page_id, false);
    return;
  }

  leaf_page->RemoveAndDeleteRecord(key, processor_);

  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    // 调整页面
    // CoalesceOrRedistribute 接收一个指向 LeafPage 的引用。
    // 它返回 true 如果给定的 node (leaf_page) 被删除了（例如合并了）。
    // 如果返回 true，则 leaf_page 的页面已经被 CoalesceOrRedistribute 或其子调用 unpin 和 delete。
    // 如果返回 false，则 leaf_page 仍然有效，被修改过（例如通过 redistribute），需要在这里 unpin。
    bool node_was_deleted_by_coalesce = CoalesceOrRedistribute(leaf_page, transaction);
    
    // =释放页面 (根据 CoalesceOrRedistribute 的结果)
    if (!node_was_deleted_by_coalesce) {
      // 如果节点未被删除 (例如，只是重新分配了)，则在此处 unpin。
      // 页面是脏的，因为原始的删除操作或后续的重新分配操作修改了它。
      buffer_pool_manager_->UnpinPage(current_leaf_page_id, true);
    }
  } else {
    buffer_pool_manager_->UnpinPage(current_leaf_page_id, true);
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  // 如果 page 是根页面 (AdjustRoot 会处理空的情况)
  if (node->IsRootPage()) {
    // 处理根页面为空的情况 (由 AdjustRoot 完成)
    // AdjustRoot 返回 true 如果 node (旧根) 被处理/删除
    return AdjustRoot(node); 
  }
  //如果 page 的条目数量大于等于最小填充
  if (node->GetSize() >= node->GetMinSize()) {
    //  返回 (表示 node 未被删除)
    return false; 
  }

  // ---- 获取父节点和兄弟节点  ---- 
  Page* parent_page_obj = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page_obj == nullptr) {
    throw std::runtime_error("Failed to fetch parent page.");
  }
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page_obj->GetData());
  page_id_t parent_page_id = parent_node->GetPageId();

  int node_index_in_parent = parent_node->ValueIndex(node->GetPageId());
  if (node_index_in_parent == -1) { 
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      throw std::runtime_error("Node not found in parent.");
  }

  page_id_t sibling_page_id;
  N* sibling_node_ptr; 
  bool is_left_sibling = false;

  if (node_index_in_parent > 0) { // 优先左兄弟
    sibling_page_id = parent_node->ValueAt(node_index_in_parent - 1);
    is_left_sibling = true;
  } else { // 否则右兄弟
    sibling_page_id = parent_node->ValueAt(node_index_in_parent + 1);
    is_left_sibling = false;
  }
  
  Page* sibling_page_obj = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (sibling_page_obj == nullptr) {
    buffer_pool_manager_->UnpinPage(parent_page_id, false);
    throw std::runtime_error("Failed to fetch sibling page.");
  }
  sibling_node_ptr = reinterpret_cast<N *>(sibling_page_obj->GetData());
  // ---- 兄弟节点获取完毕 ----

  //  如果兄弟页面可以借一个条目
  if (sibling_node_ptr->GetSize() > sibling_node_ptr->GetMinSize()) {
    // 从兄弟页面借一个条目 (执行 Redistribute)
    // Redistribute(neighbor_node, node, index) -> index = 0 左邻, index !=0 右邻
    Redistribute(sibling_node_ptr, node, is_left_sibling ? 0 : 1);

    buffer_pool_manager_->UnpinPage(parent_page_id, true); 
    buffer_pool_manager_->UnpinPage(sibling_page_id, true); 
    return false; // node 未被删除
  } else {
    // 合并 page 和兄弟页面 (执行 Coalesce)
    // Coalesce(neighbor_node, node_to_delete, parent, index_of_node_to_delete_in_parent)
    // Coalesce 返回 true 如果父节点也需要调整(例如根变空)
    bool parent_may_need_adjustment;
    parent_may_need_adjustment = Coalesce(sibling_node_ptr, node, parent_node, node_index_in_parent, transaction);

    buffer_pool_manager_->UnpinPage(sibling_page_id, true); // 兄弟节点被修改
    // node 页面已在 Coalesce 中被 unpin 和 delete

    // 如果父页面不足最小填充 (或 Coalesce 指示需要进一步调整)
    if (parent_may_need_adjustment || parent_node->GetSize() < parent_node->GetMinSize()) {
       // 递归调整父页面
       if (!CoalesceOrRedistribute(parent_node, transaction)) {
            // 如果递归调用未删除父节点 (parent_node 仍有效且被修改)
            buffer_pool_manager_->UnpinPage(parent_page_id, true); 
       }
       // 如果递归调用删除了父节点，它已在其中被 unpin
    } else {
      // 父节点未下溢，但因子女合并而被修改
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
    }
    return true; // node 被删除 (合并了)
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
                          // 将 node 的所有条目移动到 neighbor_node
                          node->MoveAllTo(neighbor_node);
                          // 更新 neighbor_node 的下一个页面 ID
                          neighbor_node->SetNextPageId(node->GetNextPageId());

                          buffer_pool_manager_->UnpinPage(node->GetPageId(),false);
                          buffer_pool_manager_->DeletePage(node->GetPageId());
                          // 从父节点中移除 node
                          parent->Remove(index);
                          // 如果父节点现在不足最小填充，或者父节点是根节点且只有一个子节点，则返回 true
                          return(parent->GetSize() < parent->GetMinSize() || parent->IsRootPage() && parent->GetSize() < 2 && !parent->IsLeafPage());
  
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
                          GenericKey *promoted_key = parent->KeyAt(index);
                          // 将 node 的所有条目移动到 neighbor_node
                          node->MoveAllTo(neighbor_node,promoted_key,buffer_pool_manager_);
                          // 删除node 
                          buffer_pool_manager_->UnpinPage(node->GetPageId(),false);
                          buffer_pool_manager_->DeletePage(node->GetPageId());
                          // 从父节点中移除 node
                          parent->Remove(index);
                          // 如果父节点现在不足最小填充，或者父节点是根节点且只有一个子节点，则返回 true
                          return (parent->GetSize() < parent->GetMinSize() ||
                                  (parent->IsRootPage() && parent->GetSize() < 2));
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  //获取父节点
  Page *parent_page_obj = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page_obj == nullptr) {
    throw std::runtime_error("Redistribute (Leaf): Failed to fetch parent page.");
  }
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page_obj->GetData());
  page_id_t parent_page_id = parent_node->GetPageId(); // For unpinning

  if (index == 0) { // neighbor_node 是 node 的左兄弟
    neighbor_node->MoveLastToFrontOf(node);
    // 更新父节点中的分隔键。
    // 该键位于指向 node 的指针的左侧，即 parent_node->KeyAt(parent_node->ValueIndex(node->GetPageId()))。
    // 新的键应该是 node 的第一个键。
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
  } else { // neighbor_node 是 node 的右兄弟 (index == 1)
    neighbor_node->MoveFirstToEndOf(node);
    // 更新父节点中的分隔键。
    // 该键位于指向 neighbor_node 的指针的左侧，即 parent_node->KeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()))。
    // 新的键应该是 neighbor_node 的第一个键（在移动之后）。
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true); // 标记父页面为脏页并解锁
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  Page *parent_page_obj = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page_obj == nullptr) {
    throw std::runtime_error("Redistribute (Internal): Failed to fetch parent page.");
  }
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page_obj->GetData());
  page_id_t parent_page_id = parent_node->GetPageId();

  if (index == 0) { // neighbor_node 是 node 的左兄弟
    // 1. 获取父节点中分隔 neighbor_node 和 node 的键 (key_from_parent)
    //    这个键位于 parent_node 中指向 node 的指针的左边
    int node_ptr_idx_in_parent = parent_node->ValueIndex(node->GetPageId());
    GenericKey *key_from_parent = parent_node->KeyAt(node_ptr_idx_in_parent);

    // 2. 记录 neighbor_node 的最后一个键，它将提升到父节点
    GenericKey *key_to_promote_up = neighbor_node->KeyAt(neighbor_node->GetSize() - 1);

    // 3. neighbor_node 将其最后一个条目移动到 node 的开头
    //    key_from_parent 会被插入到 node 中，作为新的 ValueAt(0) 和原始 ValueAt(0) 之间的分隔键
    neighbor_node->MoveLastToFrontOf(node, key_from_parent, buffer_pool_manager_);

    // 4. 更新父节点的分隔键
    parent_node->SetKeyAt(node_ptr_idx_in_parent, key_to_promote_up);

  } else { // neighbor_node 是 node 的右兄弟 (index == 1)
    // 1. 获取父节点中分隔 node 和 neighbor_node 的键 (key_from_parent)
    //    这个键位于 parent_node 中指向 neighbor_node 的指针的左边
    int neighbor_ptr_idx_in_parent = parent_node->ValueIndex(neighbor_node->GetPageId());
    GenericKey *key_from_parent = parent_node->KeyAt(neighbor_ptr_idx_in_parent);

    // 2. neighbor_node 将其第一个条目移动到 node 的末尾
    //    key_from_parent 会被插入到 node 中，作为 node 原始最后一个值和新插入的值之间的分隔键
    neighbor_node->MoveFirstToEndOf(node, key_from_parent, buffer_pool_manager_);

    // 3. neighbor_node 移动后的第一个键将提升到父节点
    //    InternalPage::MoveFirstToEndOf 内部会将原来的 KeyAt(1) 移到 KeyAt(0)
    GenericKey *key_to_promote_up = neighbor_node->KeyAt(0);
    
    // 4. 更新父节点的分隔键
    parent_node->SetKeyAt(neighbor_ptr_idx_in_parent, key_to_promote_up);
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, true); // 标记父页面为脏页并解锁
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  // 删除根页面中的最后一个元素，但根页面仍然有一个子节点
  // 对于内部节点，GetSize() 返回子指针的数量。当它为1时，表示只有一个子节点。
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    InternalPage *internal_root = reinterpret_cast<InternalPage *>(old_root_node);
    //获取并删除唯一的子页面 ID
    page_id_t child_page_id = internal_root->RemoveAndReturnOnlyChild();
    
    // 从缓冲池管理器获取子页面
    Page *child_page_obj = buffer_pool_manager_->FetchPage(child_page_id);
    if (child_page_obj == nullptr) {
        throw std::runtime_error("AdjustRoot: Failed to fetch the new root page.");
    }
    // 将子页面数据转换为 BPlusTreePage 类型 
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(child_page_obj->GetData());
    
    //  将子页面的父页面 ID 设置为 INVALID_PAGE_ID
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    
    // 更新 root_page_id_ 为子页面 ID
    root_page_id_ = child_page_id;
    
    // 调用 UpdateRootPageId(0)
    UpdateRootPageId(0); 
    
    //  释放子页面 (新的根页面)
    // 新的根页面已被修改 (ParentPageId)，所以 unpin 时标记为 dirty
    buffer_pool_manager_->UnpinPage(child_page_id, true);
    
    return true;
  }

  //  删除整个 B+ 树中的最后一个元素
  // 此时 old_root_node 是叶子页面，并且其大小为0
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    // 设置 root_page_id_ 为 INVALID_PAGE_ID
    root_page_id_ = INVALID_PAGE_ID;
    // 调用 UpdateRootPageId(0)
    UpdateRootPageId(0); 

    return true;
  }

  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  if (IsEmpty()) {
    return IndexIterator();
  }
  // 找到最左边的叶子页面
  Page *leaf_page_obj = FindLeafPage(nullptr, root_page_id_, true);
  if (leaf_page_obj == nullptr) {
    return IndexIterator(); // 树可能在FindLeafPage过程中变空或发生错误
  }
  page_id_t leaf_page_id = leaf_page_obj->GetPageId();
  // FindLeafPage 返回的页面是 pinned 的，迭代器会接管或复制所需信息，这里可以 unpin
  buffer_pool_manager_->UnpinPage(leaf_page_id, false); 
  // 迭代器从该页面的第一个条目开始
  return IndexIterator(leaf_page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  if (IsEmpty()) {
    return IndexIterator();
  }
  // 调用 FindLeafPage(key, root_page_id_, false) 获取包含 key 的叶子页面
  Page *leaf_page_obj = FindLeafPage(key, root_page_id_, false);
  // 如果页面为空
  if (leaf_page_obj == nullptr) {
    return IndexIterator();
  }
  // 将页面数据转换为 LeafPage 类型
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(leaf_page_obj->GetData());
  page_id_t leaf_page_id = leaf_page->GetPageId();
  // 获取 key 在叶子页面中的索引
  int index_in_page = leaf_page->KeyIndex(key, processor_);
  // 释放叶子页面
  // FindLeafPage 返回的页面是 pinned 的，迭代器会接管或复制所需信息，这里可以 unpin
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);

  // 返回一个新的 IndexIterator 对象
  return IndexIterator(leaf_page_id, buffer_pool_manager_, index_in_page);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(IsEmpty()){
    return nullptr;
  }
  page_id_t current_page_id = page_id;
  Page *current_page_obj = nullptr;
  if(leftMost){
    while(true){
      current_page_obj = buffer_pool_manager_->FetchPage(current_page_id);
      if(current_page_obj == nullptr){
        throw std::runtime_error("FindLeafPage: Failed to fetch page during B+ tree traversal.");
      }
      BPlusTreePage *bpt_page = reinterpret_cast<BPlusTreePage *>(current_page_obj->GetData());
      if(bpt_page->IsLeafPage()){
        return current_page_obj;
      }else{
        InternalPage *internal_page = reinterpret_cast<InternalPage *>(bpt_page);
        page_id_t next_page_id = internal_page->ValueAt(0);
        buffer_pool_manager_->UnpinPage(current_page_id,false);
        current_page_id = next_page_id;
      }
    }
  }else{
    while(true){
      current_page_obj = buffer_pool_manager_->FetchPage(current_page_id);
      if(current_page_obj == nullptr){
        throw std::runtime_error("FindLeafPage: Failed to fetch page during B+ tree traversal.");
      }
      BPlusTreePage *bpt_page = reinterpret_cast<BPlusTreePage *>(current_page_obj->GetData());
      if(bpt_page->IsLeafPage()){
        return current_page_obj;
      }else{
        InternalPage *internal_page = reinterpret_cast<InternalPage *>(bpt_page);
        page_id_t next_page_id = internal_page->Lookup(key,processor_);
        buffer_pool_manager_->UnpinPage(current_page_id,false);
        current_page_id = next_page_id;
      }
    }
  }
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  Page *page_obj = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *index_roots_page = reinterpret_cast<IndexRootsPage *>(page_obj->GetData());
  if(insert_record){
    index_roots_page->Insert(index_id_,root_page_id_);
  }else{
    index_roots_page->Update(index_id_,root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}