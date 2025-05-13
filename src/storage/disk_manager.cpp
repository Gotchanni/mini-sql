 #include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  // 获取元数据页指针，将元数据区域转换为 DiskFileMetaPage 类型
  auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  
  // 检查已分配的页面数量是否达到最大有效页面 ID
  // 如果达到最大有效页面 ID，则无法再分配新页面，返回无效页面 ID
  if (meta_page->GetAllocatedPages() >= MAX_VALID_PAGE_ID) return INVALID_PAGE_ID;

  // 查找可分配页面的扩展区（extent）
  // 获取当前元数据页中扩展区的数量
  uint32_t extent_num = meta_page->GetExtentNums();
  uint32_t extent_index;
  for(extent_index = 0 ; extent_index < extent_num ; extent_index++){
    // 检查当前扩展区是否还有可用页面
    // 如果扩展区内已使用的页面数小于位图大小，说明该扩展区还有可用页面，跳出循环
    if(meta_page->extent_used_page_[extent_index]<BITMAP_SIZE)
      break;
  }

  if (extent_index >= extent_num) {
    // 所有现有扩展区都已满，需要创建新的扩展区
    extent_index = extent_num;  // 使用新的扩展区索引
    meta_page->num_extents_ = extent_num + 1;  // 更新扩展区数量
  }

  // 计算要操作的物理页号
  // 每个扩展区内有 BITMAP_SIZE 个数据页和一个位图页，再加上元数据页就是物理页
  uint32_t physical_pageID = extent_index * (BITMAP_SIZE + 1) + 1;
  // 定义一个字符数组用于存储从物理页读取的数据
  char str[PAGE_SIZE]={'\0'};
  // 从物理页读取数据到字符数组中
  ReadPhysicalPage(physical_pageID, str);

  // 处理位图页
  // 将读取的数据转换为 BitmapPage 类型的指针
  BitmapPage<PAGE_SIZE> *bit_map = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(str);
  // 用于存储在当前位图页中分配的页面偏移量
  uint32_t page_offset = 0;
  // 在位图页中分配一个页面，并获取分配页面的偏移量
  bit_map->AllocatePage(page_offset);
  // 将更新后的位图页数据写回到物理页
  WritePhysicalPage(physical_pageID, str);
  
  // 更新元数据页信息
  // 已分配页面总数加 1
  meta_page->num_allocated_pages_++;
  meta_page->extent_used_page_[extent_index]++;
  
  // 更新扩展区数量，取当前扩展区索引加 1 和原扩展区数量的最大
  if (extent_index + 1 > extent_num) {
    meta_page->num_extents_ = extent_index + 1;
  } 
  else {
    meta_page->num_extents_ = extent_num;
  }
  return  extent_index * BITMAP_SIZE + page_offset;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  // 获取元数据页指针，将元数据区域转换为 DiskFileMetaPage 类型，以便后续操作元数据
  auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  
  // 计算对应的物理页面 ID
  // 这里的计算逻辑是根据逻辑页面 ID 来确定其在物理存储中的位置，通过一些计算得到物理页面 ID
  // 具体是逻辑页面 ID 加上 1 再加上逻辑页面 ID 除以 BITMAP_SIZE 减去逻辑页面 ID 对 BITMAP_SIZE 取模的结果
  uint32_t physical_pageID = logical_page_id + 1 + logical_page_id / BITMAP_SIZE - logical_page_id % BITMAP_SIZE;
  // 定义一个字符数组 str，大小为 PAGE_SIZE，用于存储从物理页读取的数据，初始化为全 0
  char str[PAGE_SIZE]={'\0'};
  // 从计算得到的物理页面 ID 对应的物理页中读取数据到字符数组 str 中
  ReadPhysicalPage(physical_pageID, str);

  // 处理位图页
  // 将读取到的数据（字符数组 str）转换为 BitmapPage 类型的指针 bit_map，以便操作位图页
  BitmapPage<PAGE_SIZE>* bit_map = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(str);
  // 计算在当前位图页中的页面偏移量
  // 这里是通过逻辑页面 ID 对 BITMAP_SIZE 取模来确定在当前位图页中的偏移位置
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;

  // 调用 BitmapPage 类的 DeAllocatePage 函数，传入计算得到的页面偏移量 page_offset，用于释放页面
  bit_map->DeAllocatePage(page_offset);

  // 将更新后的位图页数据（存储在字符数组 str 中）写回到计算得到的物理页面
  // 这里写回的物理页面 ID 与之前读取数据的物理页面 ID 相同
  WritePhysicalPage(logical_page_id + 1 + logical_page_id / BITMAP_SIZE - logical_page_id % BITMAP_SIZE, str);

  // 更新元数据页中已分配页面的数量，将其减 1，表示释放了一个页面
  meta_page->num_allocated_pages_--;

  // 更新元数据页中对应扩展区已使用页面的数量
  // 通过逻辑页面 ID 除以 BITMAP_SIZE 确定所属扩展区，然后将该扩展区已使用页面数量减 1
  meta_page->extent_used_page_[logical_page_id / BITMAP_SIZE]--;
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
// 检查逻辑页面 ID 是否大于最大有效页面 ID
if (logical_page_id > MAX_VALID_PAGE_ID) {
  // 如果大于最大有效页面 ID，直接返回 false，表明该页面相关操作无法正常进行
  return false;
}

// 定义一个字符数组 str，用于存储从物理页读取的数据，其大小为 PAGE_SIZE，初始化为全 0
char str[PAGE_SIZE] = {'\0'};

// 计算逻辑页面 ID 相关的物理页面 ID，具体计算方式为逻辑页面 ID 加上 1 加上逻辑页面 ID 除以 BITMAP_SIZE 减去逻辑页面 ID 对 BITMAP_SIZE 取模的结果
// 这里的计算是为了确定逻辑页面在物理存储中的对应位置
uint32_t physical_pageID = logical_page_id + 1 + logical_page_id / BITMAP_SIZE - logical_page_id % BITMAP_SIZE;

// 从计算得到的物理页面 ID 对应的物理页中读取数据到字符数组 str 中
// 假设 ReadPhysicalPage 函数用于从指定物理页读取数据到传入的字符数组中
ReadPhysicalPage(physical_pageID, str);

// 处理位图页
// 将读取到数据的字符数组 str 转换为 BitmapPage 类型的指针 bit_map
// 这样转换是为了能够使用 BitmapPage 类中的相关方法来处理位图页数据
BitmapPage<PAGE_SIZE>* bit_map = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(str);

// 调用 BitmapPage 类的 IsPageFree 方法，传入逻辑页面 ID 对 BITMAP_SIZE 取模的结果
// 该方法用于判断对应位置的页面是否空闲，并返回判断结果
return bit_map->IsPageFree(logical_page_id % BITMAP_SIZE);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id + 1 + logical_page_id / BITMAP_SIZE + 1;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_pageID, char *page_data) {
  int offset = physical_pageID * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_pageID, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_pageID) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}