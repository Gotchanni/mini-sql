#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int low = 0,high = GetSize() -1;
  while(low<=high){
    int mid = (low + high)/2;
    if(KM.CompareKeys(KeyAt(mid),key)>=0){
      high = mid - 1;
    }else{
      low = mid + 1;
    }
  }
  return low;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  // 若值已经存在则直接返回当前大小
  if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {
    return GetSize();
  }
  int size = GetSize();
  // 将index及之后的元素后移
  for (int i = size; i > index; i--) {
    PairCopy(pairs_off + i * pair_size, pairs_off + (i - 1) * pair_size);
  }
  
  // 插入新键值对
  SetKeyAt(index, key);          // 设置新键
  SetValueAt(index, value);      // 设置新值
  IncreaseSize(1);               // 增加页面元素计数
  
  return GetSize();              // 返回插入后的新大小
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int size = GetSize();
  int half = size / 2;
  recipient->CopyNFrom(pairs_off+half*pair_size,size-half);
  SetSize(half);
  recipient->SetNextPageId(this->GetNextPageId());//TO CHECK:更新next_page_id/NextPageId
  this->SetNextPageId(recipient->GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  int oldsize = GetSize();
  memcpy(PairPtrAt(oldsize),src,static_cast<size_t>(size)*(GetKeySize() + sizeof(RowId)));
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key,KM);
  if(index<GetSize() && KM.CompareKeys(KeyAt(index),key)==0){
    value = ValueAt(index);
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int index = KeyIndex(key,KM),size = GetSize();
  if(index<size && KM.CompareKeys(KeyAt(index),key)==0){
    for(int i=index;i< size-1; i++){
      PairCopy(PairPtrAt(i),PairPtrAt(i+1),1);
    }
    IncreaseSize(-1);
    size--;
  }
  return(size);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  int size =GetSize();
  recipient->CopyNFrom(PairPtrAt(0),size);
  recipient->SetNextPageId(next_page_id_);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  int size = GetSize();
  if(size == 0) return ;
  GenericKey* key_to_move = KeyAt(0);
  RowId value_to_move = ValueAt(0);
  recipient->CopyLastFrom(key_to_move,value_to_move);
  for(int i = 0;i<size-1;i++){
    PairCopy(PairPtrAt(i),PairPtrAt(i+1),1);
  }
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int size = GetSize();
  SetKeyAt(size,key);
  SetValueAt(size,value);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  int size = GetSize();
  if(size == 0) return;
  GenericKey* ket_to_move = KeyAt(GetSize()-1);
  RowId value_to_move = ValueAt(GetSize() - 1);
  recipient->CopyFirstFrom(ket_to_move,value_to_move);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int current_size = GetSize();
  for (int i = current_size; i > 0; --i) {
    PairCopy(PairPtrAt(i), PairPtrAt(i - 1), 1);
  }
  SetKeyAt(0, key);
  SetValueAt(0, value);
  IncreaseSize(1);
}