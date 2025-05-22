#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn): table_heap_(table_heap), current_row_id_(rid), txn_(txn) {
  if(rid == INVALID_ROWID) 
    return;
  ASSERT(table_heap_, "TableHeap is nullptr.");
  Row* row = new Row(current_row_id_);
  table_heap_->GetTuple(row, nullptr);
  ASSERT(row, "Invalid row.");
  current_row_ = *row;
  // bool result = table_heap_->GetTuple(&current_row_,txn_);
  // ASSERT(result, "Failed to fetch tuple at table iterator init");
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  current_row_id_ = other.current_row_id_;
  txn_ = other.txn_;
  current_row_=other.current_row_;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (table_heap_  == itr.table_heap_) && (current_row_id_ == itr.current_row_id_);
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !((table_heap_  == itr.table_heap_) && (current_row_id_ == itr.current_row_id_));
}

const Row &TableIterator::operator*() {
  return current_row_;
}

Row *TableIterator::operator->() {
  return &current_row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_  = itr.table_heap_;
  current_row_ = itr.current_row_;
  current_row_id_ = itr.current_row_id_;  
  txn_ = itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (current_row_id_ == INVALID_ROWID || table_heap_ == nullptr) 
    return *this;

  auto buf_pool = table_heap_->buffer_pool_manager_;
  page_id_t current_page_id = current_row_id_.GetPageId();

  auto page = reinterpret_cast<TablePage *>(buf_pool->FetchPage(current_page_id));

  RowId next_row_id;
  

  if (page->GetNextTupleRid(current_row_id_, &next_row_id)) {

    buf_pool->UnpinPage(current_page_id, false);
    current_row_id_ = next_row_id;

    Row new_row(current_row_id_);
    bool result = table_heap_->GetTuple(&new_row, txn_);
    ASSERT(result, "TableIterator::operator++: GetTuple failed");

    current_row_ = new_row;
    return *this;
  }

  page_id_t next_page_id = page->GetNextPageId();
  buf_pool->UnpinPage(current_page_id, false);
  while (next_page_id != INVALID_PAGE_ID) {
    auto page_ = reinterpret_cast<TablePage *>(buf_pool->FetchPage(next_page_id));

    if (page_->GetFirstTupleRid(&next_row_id)) {
      buf_pool->UnpinPage(next_page_id, false);
      current_row_id_ = next_row_id;

      Row new_row(current_row_id_);
      bool result = table_heap_->GetTuple(&new_row, txn_);
      ASSERT(result, "TableIterator::operator++: GetTuple failed on new page");

      current_row_ = new_row;
      return *this;
    }
    page_id_t next_page_id_temp = page_->GetNextPageId(); 
    buf_pool->UnpinPage(next_page_id, false);
    next_page_id = next_page_id_temp;
  }

  current_row_id_ = INVALID_ROWID;
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) { 
  TableIterator tmp(*this);
  ++(*this);
  return tmp;  
}
