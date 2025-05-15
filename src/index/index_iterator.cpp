#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

/**
 * TODO: Student Implement
 */
std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  if (current_page_id == INVALID_PAGE_ID || page == nullptr) {
    throw std::out_of_range("Cannot dereference an invalid or end iterator.");
  }
  return page->GetItem(item_index);
}

/**
 * TODO: Student Implement
 */
IndexIterator &IndexIterator::operator++() {
  // 检查迭代器是否已经无效或指向末尾
  if (current_page_id == INVALID_PAGE_ID || page == nullptr) {
    // 如果已经是结束迭代器或无效，不进行任何操作
    return *this;
  }
  // 尝试移动到当前页面的下一个条目
  item_index++;

  // 检查是否仍在当前页面内
  if (item_index < page->GetSize()) {
    return *this; // 成功移动到当前页面的下一条目
  }
  // 如果超出了当前页面的范围，需要移动到下一个叶子页面
  page_id_t next_page_id = page->GetNextPageId();
  // Unpin 当前页面，因为我们要离开它了
  buffer_pool_manager->UnpinPage(current_page_id, false);

  // 更新到下一个页面的信息
  current_page_id = next_page_id;
  item_index = 0; // 到新页面后，从第一个条目开始

  if (current_page_id == INVALID_PAGE_ID) {
    // 没有更多页面了，到达B+树的末尾
    page = nullptr; // 将page指针置空
  } else {
    // 尝试获取新的叶子页面
    Page *raw_new_page = buffer_pool_manager->FetchPage(current_page_id);
    if (raw_new_page == nullptr) {
      // 获取新页面失败，视为到达末尾
      page = nullptr;
      current_page_id = INVALID_PAGE_ID; // 确保状态一致性
    } else {
      // 成功获取新页面
      page = reinterpret_cast<LeafPage *>(raw_new_page->GetData());
    }
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}