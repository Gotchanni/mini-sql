#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  uint32_t  tuple_size = row.GetSerializedSize(schema_);
  if (tuple_size > PAGE_SIZE) {
    return false; 
  }
  // Step1: Find the page which can insert the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Step2: Insert the tuple into the page.
  page->WLatch();
  bool result = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  // Step3: If the page is full, then create a new page and link it to the old page.
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  while (!result) {
    page_id_t new_page_id=page->GetNextPageId();
    if (new_page_id != INVALID_PAGE_ID) {
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(new_page_id));
      page->WLatch();
      result = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    } else {
      page_id_t new_page_id_;
      buffer_pool_manager_->NewPage(new_page_id_);
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(new_page_id_));
      new_page->WLatch();
      new_page->Init(new_page_id_, page->GetPageId(),log_manager_,txn);
      page->WLatch();
      page->SetNextPageId(new_page_id_);
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      result = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      new_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
      break;
    }
  }
  if (result) {
    // Step4: If the tuple is inserted successfully, then return true.
    return true;
  } else {
    // Step5: If the tuple is not inserted successfully, then return false.
    return false;
  }
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { 
  // Step1: Find the page which contains the tuple.
  auto old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  
  // Step2: Update the tuple in the page.
  old_page->WLatch();
  Row old_row = Row(rid);
  bool result = old_page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  old_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(old_page->GetPageId(), true);
  return result;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page not found, abort the transaction.
  ASSERT(page!= nullptr,"page not found when delete");
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid,txn,log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  page->RLatch();
  bool result = page->GetTuple(row,schema_,txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  return result;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { 
  auto tmp_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId rid;
  tmp_page->GetFirstTupleRid(&rid);
  buffer_pool_manager_->UnpinPage(tmp_page->GetPageId(), false); 
  return TableIterator(this, rid, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { 
  return TableIterator(this, INVALID_ROWID, nullptr);
}
