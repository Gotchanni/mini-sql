#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // 如果已经分配了所有页面，返回false
  if (page_allocated_ >= GetMaxSupportedSize()) {
    return false;
  }
  uint32_t next_byte_index = next_free_page_ / 8;
  uint8_t next_bit_index = next_free_page_ % 8;
  //标记当前页面为已分配
  bytes[next_byte_index] |= (1 << next_bit_index);
  //返回分配页面的偏移量
  page_offset = next_free_page_;
  //寻找下一个可用页面
  for(uint32_t i=0;i<MAX_CHARS*8;i++){
    uint32_t byte_index = i / 8;
    uint8_t bit_index = i % 8;
    if(IsPageFreeLow(byte_index,bit_index)){
      next_free_page_ = i;
      break ;
    }
  }
  //更新已分配页面数量并返回结果
  page_allocated_++;
  return true;
  
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  // 检查 page_offset 是否有效
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  }

  // 计算字节索引= page_offset/8 和位索引page_offset%8
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;

  // 检查该页是否已被分配
  if (IsPageFreeLow(byte_index, bit_index)) {
    return false;  // 该页本来就是空闲的，无法回收
  }
  // 将对应位设置为0（空闲）
  bytes[byte_index] &= ~(1 << bit_index);
  // 更新已分配页面计数
  page_allocated_--;
  next_free_page_ = page_offset;

  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  // 检查 page_offset 是否有效
  if (page_offset >= GetMaxSupportedSize()) {
    return false;  // 无效的偏移量，视为已分配
  }
  // 计算字节索引和位索引
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;

  // 调用 IsPageFreeLow 检查该位是否为0（空闲）
  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if (byte_index >= MAX_CHARS || bit_index >= 8) {
    return false;  // 无效的参数，视为已分配
  }

  // 检查该位是否为0（空闲）
  return (bytes[byte_index] & (1 << bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;