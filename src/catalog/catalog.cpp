#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  // 魔数(4) + 表数(4) + 索引数(4) + 表信息(8/个) + 索引信息(8/个)
  return 4 + 4 + 4 + table_meta_pages_.size() * 8 + index_meta_pages_.size() * 8;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    next_table_id_ = 0;
    next_index_id_ = 0;
  } else {
    Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);

    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();

    for (auto &table_meta : *(catalog_meta_->GetTableMetaPages())) {
      LoadTable(table_meta.first, table_meta.second);
    }

    for (auto &index_meta : *(catalog_meta_->GetIndexMetaPages())) {
      LoadIndex(index_meta.first, index_meta.second);
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }

  table_id_t table_id = next_table_id_++;

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  if (table_heap == nullptr) {
    return DB_FAILED;
  }

  page_id_t meta_page_id;
  Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);
  if (meta_page == nullptr) {
    delete table_heap;
    return DB_FAILED;
  }

  TableSchema *table_schema = Schema::DeepCopySchema(schema); //深拷贝
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), table_schema);
  
  table_meta->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(meta_page_id, true);

  catalog_meta_->table_meta_pages_[table_id] = meta_page_id;

  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);

  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;

  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  tables.clear();

  for (const auto &pair : tables_) {
    tables.push_back(pair.second);
  }

  return tables.empty() ? DB_FAILED : DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  TableInfo *table_info;
  if (GetTable(table_name, table_info) != DB_SUCCESS) {
    return DB_TABLE_NOT_EXIST;
  }

  if (index_names_.find(table_name) != index_names_.end() && index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
    return DB_INDEX_ALREADY_EXIST;
  }

  auto schema = table_info->GetSchema();
  std::vector<uint32_t> key_map;

  for (const auto &key : index_keys) {
    uint32_t column_index;
    if (schema->GetColumnIndex(key, column_index) != DB_SUCCESS) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(column_index);
  }

  page_id_t meta_page_id;
  Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);
  if (meta_page == nullptr) {
    return DB_FAILED;
  }

  index_id_t index_id = next_index_id_++;

  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_info->GetTableId(), key_map);

  index_meta->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(meta_page_id, true);

  catalog_meta_->index_meta_pages_[index_id] = meta_page_id;

  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  if (index_names_.find(table_name) == index_names_.end()) {
    index_names_[table_name] = std::unordered_map<std::string, index_id_t>();
  }
  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = index_info;

  TableIterator iter = table_info->GetTableHeap()->Begin(txn);
  while (iter != table_info->GetTableHeap()->End()) {
    const Row &row = *iter;
    index_info->GetIndex()->InsertEntry(row, row.GetRowId(), txn);
    ++iter;
  }

  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (index_names_.find(table_name) == index_names_.end() || index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) {
    return DB_INDEX_NOT_FOUND;
  }

  index_id_t index_id = index_names_.at(table_name).at(index_name);
  index_info = indexes_.at(index_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  indexes.clear();

  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  for (const auto &pair : index_names_.at(table_name)) {
    index_id_t index_id = pair.second;
    auto index_it = indexes_.find(index_id);
    if (index_it != indexes_.end()) {
      indexes.push_back(index_it->second);
    }
  }

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info = tables_[table_id];

  std::vector<IndexInfo *> indexes;
  if (GetTableIndexes(table_name, indexes) == DB_SUCCESS) {
    for (auto index_info : indexes) {
      DropIndex(table_name, index_info->GetIndexName());
    }
  }

  page_id_t meta_page_id = catalog_meta_->table_meta_pages_[table_id];
  catalog_meta_->table_meta_pages_.erase(table_id);

  buffer_pool_manager_->DeletePage(meta_page_id);

  table_names_.erase(table_name);
  tables_.erase(table_id);

  delete table_info;

  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) {
    return DB_INDEX_NOT_FOUND;
  }

  index_id_t index_id = index_names_.at(table_name).at(index_name);
  IndexInfo *index_info = indexes_.at(index_id);

  index_names_.at(table_name).erase(index_name);
  if (index_names_.at(table_name).empty()) {
    index_names_.erase(table_name);
  }
  indexes_.erase(index_id);

  page_id_t meta_page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);

  buffer_pool_manager_->DeletePage(meta_page_id);

  delete index_info;

  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (catalog_meta_page == nullptr) {
    return DB_FAILED;
  }
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page* meta_page = buffer_pool_manager_->FetchPage(page_id);
  if (meta_page == nullptr) {
    return DB_FAILED;
  }

  TableMetadata *table_meta = nullptr;
  TableMetadata::DeserializeFrom(meta_page->GetData(), table_meta);
  buffer_pool_manager_->UnpinPage(page_id, false);

  if (table_meta == nullptr) {
    return DB_FAILED;
  }

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_);

  TableInfo * table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);

  table_names_[table_meta->GetTableName()] = table_id;
  tables_[table_id] = table_info;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  Page *index_meta_page = buffer_pool_manager_->FetchPage(page_id);
  if (index_meta_page == nullptr) {
    return DB_FAILED;
  }

  IndexMetadata *index_meta = nullptr;
  IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
  buffer_pool_manager_->UnpinPage(page_id, false);

  if (index_meta->GetIndexId() != index_id) {
    delete index_meta;
    return DB_FAILED;
  }

  table_id_t table_id = index_meta->GetTableId();
  TableInfo *table_info = nullptr;
  if (GetTable(table_id, table_info) != DB_SUCCESS) {
    delete index_meta;
    return DB_TABLE_NOT_EXIST;
  }

  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  std::string table_name = "";
  for (auto &entry : table_names_) {
    if (entry.second == table_id) {
      table_name = entry.first;
      break;
    }
  }

  if (table_name.empty()) {
    delete index_info;
    return DB_TABLE_NOT_EXIST;
  }

  indexes_[index_id] = index_info;
  index_names_[table_name][index_meta->GetIndexName()] = index_id;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  table_info = tables_[table_id];
  return DB_SUCCESS;
}