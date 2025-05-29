#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t tmp;
  if(page_id > MAX_VALID_PAGE_ID) return nullptr;
  if(page_id <= INVALID_PAGE_ID) return nullptr;

  // 查询page_table_，如果存在则直接返回
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    tmp = it->second;
    replacer_->Pin(tmp);
    pages_[tmp].pin_count_++;
    return &pages_[tmp];
  }
  // 处理空闲列表
  if (!free_list_.empty()) {
    tmp = free_list_.front();
    free_list_.pop_front(); // 提前更新free_list_
  
    page_table_[page_id] = tmp;
    pages_[tmp].ResetMemory(); 
    pages_[tmp].page_id_ = page_id;
    pages_[tmp].pin_count_ = 1;
  
    disk_manager_->ReadPage(page_id, pages_[tmp].data_);
    return &pages_[tmp];
  }

  // 处理替换策略
  if (!replacer_->Victim(&tmp)) {
    return nullptr;
  }

  // 处理脏页写回
  if (pages_[tmp].IsDirty()) {
    disk_manager_->WritePage(pages_[tmp].GetPageId(), pages_[tmp].GetData());
  }

  // 更新页表和页面元数据
  page_table_.erase(pages_[tmp].page_id_); // 移除旧映射
  pages_[tmp].ResetMemory(); // 重置页面内容
  pages_[tmp].page_id_ = page_id;
  pages_[tmp].pin_count_ = 1;
  page_table_[page_id] = tmp;

  disk_manager_->ReadPage(page_id, pages_[tmp].data_);
  return &pages_[tmp];

  }
/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = 0;
  frame_id_t tmp;
  //如果free_list_不为空，则从free_list_中获取一个空闲页
  
  if(!free_list_.empty()){
    page_id = AllocatePage();
    tmp = free_list_.front();
    free_list_.pop_front();
  
  }
  //如果free_list_为空，则从replacer_中获取一个替换页
  else if (!replacer_->Victim(&tmp)) 
    return nullptr;  // 无可用替换页
  else{
    
    page_id = AllocatePage();
    //如果替换页是脏页，则写回磁盘
    if(pages_[tmp].IsDirty()){
      disk_manager_->WritePage(pages_[tmp].GetPageId(),pages_[tmp].GetData());
    }
    //从page_table_中删除替换页
    page_table_.erase(pages_[tmp].page_id_);
  }
  //更新元数据
  pages_[tmp].ResetMemory();
  pages_[tmp].page_id_ = page_id;
  pages_[tmp].pin_count_ = 1;
  page_table_[page_id] = tmp;
  return &pages_[tmp];
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(page_table_.find(page_id) == page_table_.end())
    return true;
  //从page_table_中获取页号
  frame_id_t tmp = page_table_[page_id];
  //如果页号的pin_count_不为0，则返回false
  //表示该页正在被使用
  if(pages_[tmp].pin_count_>0){
    std::cout<<"0-Error"<<std::endl;
    return false;
  }
  //从replacer_中删除该页
  page_table_.erase(page_id);
  pages_[tmp].ResetMemory();
  pages_[tmp].page_id_=INVALID_PAGE_ID;
  pages_[tmp].is_dirty_=false;
  free_list_.push_back(tmp);
  disk_manager_->DeAllocatePage(page_id);//call DeallocatePage
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  //查询page_table_，如果不存在则返回false
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  //从page_table_中获取页号
  frame_id_t tmp = page_table_[page_id];
  if(pages_[tmp].pin_count_==0) {
    // std::cout<<"Unpin-Warning"<<std::endl;
    return true;
  }
  //如果页号的pin_count_不为0，则将其pin_count_减1
  pages_[tmp].pin_count_--;
  //如果页号的pin_count_为0，则将其从replacer_中删除
  replacer_->Unpin(tmp);
  //如果is_dirty为true，则将其is_dirty_设置为true
  if(is_dirty) 
    pages_[tmp].is_dirty_ = true;
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  latch_.lock();//加锁
  // 快速检查：页表中不存在则直接返回
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  // 获取帧ID并写入磁盘
  frame_id_t tmp = it->second;
  disk_manager_->WritePage(page_id, pages_[tmp].data_);
  latch_.unlock();
  return true;
  }

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}