#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  // The first key (KeyAt(0)) is invalid and is ignored as per class contract.
  // Effective keys for comparison are KeyAt(1) through KeyAt(GetSize()-1).
  // ValueAt(0) points to the child page for keys strictly less than KeyAt(1).
  // ValueAt(i) (for i >= 1) points to the child page for keys where KeyAt(i) <= key.
  // More specifically, ValueAt(i) is for keys k such that KeyAt(i) <= k < KeyAt(i+1) (if KeyAt(i+1) exists),
  // and ValueAt(GetSize()-1) is for keys k >= KeyAt(GetSize()-1).

  // ans_val_idx will store the index of the correct ValueAt() to return.
  // It's initialized to 0, meaning if the key is less than KeyAt(1) (or if no KeyAt(1) exists),
  // we take the leftmost child pointer ValueAt(0).
  int ans_val_idx = 0;

  // Binary search over the effective key indices: [1, GetSize()-1].
  int low_key_idx = 1;
  int high_key_idx = GetSize() - 1;

  while (low_key_idx <= high_key_idx) {
    int mid_key_idx = low_key_idx + (high_key_idx - low_key_idx) / 2;
    if (KM.CompareKeys(key, KeyAt(mid_key_idx)) < 0) {
      // key < KeyAt(mid_key_idx)
      // The target key is in the range covered by a pointer to the left of ValueAt(mid_key_idx).
      // The current ans_val_idx (e.g., 0 or a previous mid_key_idx where key was >=) is still the best candidate so far for this left range.
      high_key_idx = mid_key_idx - 1;
    } else {
      // key >= KeyAt(mid_key_idx)
      // This means ValueAt(mid_key_idx) is a potential candidate child page.
      // Update ans_val_idx to mid_key_idx, as keys in the subtree of ValueAt(mid_key_idx) start with KeyAt(mid_key_idx).
      ans_val_idx = mid_key_idx;
      // Continue searching in the right half to see if an even larger key (KeyAt(j), j > mid_key_idx) is also <= input key.
      low_key_idx = mid_key_idx + 1;
    }
  }
  return ValueAt(ans_val_idx);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // 第一个键 KeyAt(0) 被认为是无效的或占位键。
  // old_value 是指向左子节点（即原来的旧根节点）的页面ID。
  SetValueAt(0, old_value); // 设置第一个指针，指向旧的根页面

  // new_key 是第一个实际的分隔键。
  // new_value 是指向右子节点（即分裂产生的新页面）的页面ID。
  SetKeyAt(1, new_key);     // 设置第一个有效键 (KeyAt(1))
  SetValueAt(1, new_value); // 设置第二个指针，指向新分裂出的页面

  // 新的根页面现在包含两个 (键, 指针) 对。
  // 概念上是 (无效键, old_value) 和 (new_key, new_value)。
  // 因此，页面的大小 (size_) 设置为 2，表示有2个子指针。
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // 1. 查找 old_value 对应的索引
  int idx_of_old_value = ValueIndex(old_value);
  // new_key 和 new_value 将插入到 idx_of_old_value + 1 的位置
  int insertion_point = idx_of_old_value + 1;

  // 2. 将从 insertion_point 开始的所有 (键,值) 对向右移动一位，为新条目腾出空间
  // 从最后一个元素开始向右移动，避免覆盖还未移动的数据
  for (int i = GetSize() - 1; i >= insertion_point; --i) {
    SetKeyAt(i + 1, KeyAt(i));       // 移动键
    SetValueAt(i + 1, ValueAt(i)); // 移动值 (子页面指针)
  }

  // 3. 在 insertion_point 处插入新的 (键,值) 对
  SetKeyAt(insertion_point, new_key);
  SetValueAt(insertion_point, new_value);

  // 4. 增加当前页面的大小
  IncreaseSize(1);

  // 5. 返回新的页面大小
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half_size = GetSize() /2;
  recipient->CopyNFrom(pairs_off + half_size * pair_size,GetSize() - half_size,buffer_pool_manager);
  SetSize(half_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  // src: 源数据指针，指向要复制的第一个 (Key,Value) 对。
  // size: 要复制的 (Key,Value) 对的数量。
  // buffer_pool_manager: 用于获取和更新子页面。

  int old_current_size = GetSize(); // 获取当前页面已有的 (Key,Value) 对数量

  // 1. 将 'size' 个 (Key,Value) 对从 'src' 复制到当前页面 data_ 区域的末尾。
  // PairPtrAt(old_current_size) 指向当前已有数据之后的位置。
  memcpy(PairPtrAt(old_current_size), src, static_cast<size_t>(size) * (GetKeySize() + sizeof(page_id_t)));

  // 2. 更新被新追加的所有子页面的父页面ID，使其指向当前页面。
  // 这些新追加的条目位于索引 old_current_size 到 old_current_size + size - 1。
  for (int i = 0; i < size; ++i) {
    page_id_t child_page_id = ValueAt(old_current_size + i); // 获取第 i 个新追加条目的子页面ID
    Page *child_raw_page = buffer_pool_manager->FetchPage(child_page_id);
    if (child_raw_page == nullptr) {
      // 理论上，这里不应该发生，因为这些子页面ID应该是有效的。
      // 可以添加错误处理或断言。
      continue;
    }
    // 将子页面转换为 BPlusTreePage 类型，以便调用 SetParentPageId。
    BPlusTreePage *child_bpt_page = reinterpret_cast<BPlusTreePage *>(child_raw_page->GetData());
    child_bpt_page->SetParentPageId(GetPageId()); // 将子页面的父ID更新为当前页面的ID

    // 解除对子页面的锁定，并标记为脏页（因为父ID已修改）。
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }

  // 3. 增加当前页面的大小，加上新追加的条目数量。
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  for(int i = index; i<GetSize()-1;i++){
    SetKeyAt(i,KeyAt(i+1));
    SetValueAt(i,ValueAt(i+1));
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  page_id_t child_page_id = ValueAt(0);
  SetSize(0);
  return child_page_id;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  // 'this' 是源页面，其所有内容将被移动到 'recipient'。
  // 'middle_key' 是来自父节点的键，它原先分隔了 'this' 和 'recipient' 代表的键范围。
  // 此键需要被整合到 'recipient' 中。

  int source_original_size = GetSize(); // 获取源页面中的 (键,值) 对总数。

  if (source_original_size == 0) {
    return; // 如果源页面为空，则无需操作。
  }

  // 1. 用来自父节点的 middle_key 替换源页面 (this) 的第一个键 (KeyAt(0))。
  //    源页面的 ValueAt(0) (第一个子指针) 保持不变，现在与 middle_key 关联。
  this->SetKeyAt(0, middle_key);

  // 2. 将源页面 (this) 的全部内容 (现在第一个键已更新为 middle_key)
  //    通过 CopyNFrom 追加到 recipient 页面的末尾。
  //    CopyNFrom 会负责:
  //      a. 将数据追加到 recipient 的末尾。
  //      b. 更新所有被移动条目指向的子页面的父ID为 recipient->GetPageId()。
  //      c. 增加 recipient 的大小。
  recipient->CopyNFrom(this->PairPtrAt(0), source_original_size, buffer_pool_manager);

  // 3. 清空源页面 ('this')，因为其所有内容都已移走。
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  // 'this' 是源页面，它将其第一个逻辑条目移动到 'recipient' 页面的末尾。
  // 'recipient' 是目标页面 (通常是 'this' 的左兄弟)。
  // 'middle_key' 是从父节点传入的、原先分隔 'recipient' 和 'this' 的键。
  //   这个 middle_key 将成为 recipient 中追加的新条目的键部分。
  // 'buffer_pool_manager' 用于更新子页面的父ID。

  if (GetSize() == 0) { // 如果源页面为空，则无法移动第一个条目。
    return;
  }

  // 1. 获取源页面 ('this') 的第一个子页面ID。
  //    这是要和 middle_key 一起移动到 recipient 的值部分。
  page_id_t first_child_pid_to_move = ValueAt(0);

  // 2. 调用 recipient 的 CopyLastFrom 方法，
  //    将 (middle_key, first_child_pid_to_move) 追加到 recipient 的末尾。
  //    CopyLastFrom 会处理子页面父ID的更新和 recipient 大小的增加。
  recipient->CopyLastFrom(middle_key, first_child_pid_to_move, buffer_pool_manager);

  // 3. 从源页面 ('this') 中移除其第一个逻辑条目。
  //    这涉及到将所有条目向左移动一个位置。
  //    原先的 KeyAt(1) 变为新的 KeyAt(0) (内容上，逻辑上 KeyAt(0) 仍无效，但其内容很重要，是新的 middle_key)。
  //    原先的 ValueAt(1) 变为新的 ValueAt(0)。
  //    ...以此类推。
  for (int i = 0; i < GetSize() - 1; ++i) {
    SetKeyAt(i, KeyAt(i + 1));
    SetValueAt(i, ValueAt(i + 1));
  }

  // 4. 更新源页面 ('this') 的大小。
  IncreaseSize(-1);

  // 注意：此操作完成后，原先 this->KeyAt(1) 的内容 (现在位于 this->KeyAt(0)) 
  // 是应该更新回父节点的新的 middle_key。此函数不直接返回它，
  // 上层调用逻辑需要知道从 this->KeyAt(0) 获取这个新 middle_key。
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // 此函数由 recipient 页面调用，用于在其数据末尾追加一个新的 (key, value) 对。
  // key: 要追加的键。
  // value: 要追加的子页面ID。
  // buffer_pool_manager: 用于更新子页面的父ID。

  int current_idx = GetSize(); // 新条目将被放置的索引位置 (即当前大小)

  // 1. 在页面末尾设置新的键和值。
  SetKeyAt(current_idx, key);
  SetValueAt(current_idx, value);

  // 2. 更新新追加的子页面 (由 value 指向) 的父页面ID。
  Page *child_raw_page = buffer_pool_manager->FetchPage(value);
  if (child_raw_page == nullptr) {
    // 错误处理或断言
    return;
  }
  BPlusTreePage *child_bpt_page = reinterpret_cast<BPlusTreePage *>(child_raw_page->GetData());
  child_bpt_page->SetParentPageId(GetPageId()); // 设置父ID为当前页面 (recipient)
  buffer_pool_manager->UnpinPage(value, true);  // Unpin并标记为脏页

  // 3. 增加当前页面的大小。
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient's array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  // 'this' 是源页面，它将其最后一个条目移动到 'recipient' 页面的开头。
  // 'recipient' 是目标页面 (通常是 'this' 的右兄弟)。
  // 'middle_key' 是从父节点传入的、原先分隔 'this' 和 'recipient' 的键。
  //   这个 middle_key 将成为 recipient 中、在新插入的 ValueAt(0) 之后的第一个有效键 KeyAt(1)。
  // 'buffer_pool_manager' 用于更新子页面的父ID。

  if (GetSize() == 0) { // 如果源页面为空，则无法移动最后一个条目。
    return;
  }

  // 1. 获取源页面 ('this') 的最后一个子页面ID。
  //    这是要移动到 recipient 开头的 ValueAt(0) 的值。
  page_id_t last_child_pid_to_move = ValueAt(GetSize() - 1);
  //    获取源页面 ('this') 的最后一个键，这个键将成为新的 middle_key 更新回父节点。
  GenericKey *new_middle_key_content = KeyAt(GetSize() - 1);

  // 2. 调用 recipient 的 CopyFirstFrom 方法，
  //    将 last_child_pid_to_move 作为 recipient->ValueAt(0) 插入，并将其他元素右移。
  //    CopyFirstFrom 会处理子页面父ID的更新和 recipient 大小的增加。
  recipient->CopyFirstFrom(last_child_pid_to_move, buffer_pool_manager);

  // 3. 在 recipient 页面中，将父节点传入的 middle_key 设置到 KeyAt(1) 的位置。
  //    这是新插入的 ValueAt(0) 右边的第一个有效键。
  recipient->SetKeyAt(1, middle_key);

  // 4. 从源页面 ('this') 中移除其最后一个条目。
  IncreaseSize(-1);

  // 注意：此操作完成后，原先 this->KeyAt(GetSize()) (在未减小size前) 的内容
  // 即上面保存的 new_middle_key_content，是应该更新回父节点的新的 middle_key。
  // 上层调用逻辑需要知道从 new_middle_key_content 获取这个新 middle_key。
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // 此函数由 recipient 页面调用，用于在其数据开头插入一个新的条目，值为 value。
  // key 部分 KeyAt(0) 保持/变为无效键。
  // value: 要插入的子页面ID。
  // buffer_pool_manager: 用于更新子页面的父ID。

  // 1. 将页面中所有现有条目向右移动一个位置，为新条目腾出空间。
  //    从最后一个条目开始向后移动。
  for (int i = GetSize() - 1; i >= 0; --i) {
    SetKeyAt(i + 1, KeyAt(i));
    SetValueAt(i + 1, ValueAt(i));
  }

  // 2. 在页面开头 (索引0) 设置新的值。
  //    KeyAt(0) 按照约定是无效的，不需要显式设置一个特定键，除非有特殊占位符。
  SetValueAt(0, value);

  // 3. 更新新插入的子页面 (由 value 指向) 的父页面ID。
  Page *child_raw_page = buffer_pool_manager->FetchPage(value);
  if (child_raw_page == nullptr) {
    // 错误处理或断言
    return;
  }
  BPlusTreePage *child_bpt_page = reinterpret_cast<BPlusTreePage *>(child_raw_page->GetData());
  child_bpt_page->SetParentPageId(GetPageId()); // 设置父ID为当前页面 (recipient)
  buffer_pool_manager->UnpinPage(value, true);  // Unpin并标记为脏页

  // 4. 增加当前页面的大小。
  IncreaseSize(1);
}