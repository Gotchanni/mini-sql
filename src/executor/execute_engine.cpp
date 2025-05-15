#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect)
      delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif

  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }

  pSyntaxNode table_name_node = ast->child_;
  string table_name = table_name_node->val_;

  TableInfo *table_info = nullptr;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_SUCCESS) {
    return DB_TABLE_ALREADY_EXIST;
  }

  vector<Column *> columns;
  vector<string> primary_keys;

  pSyntaxNode column_list = table_name_node->next_;
  if (column_list == nullptr || column_list->child_ == nullptr) {
    return DB_FAILED;
  }
  pSyntaxNode column_node = column_list->child_;
  uint32_t column_index = 0;

  while(column_node != nullptr) {
    if (column_node->type_ == kNodeColumnDefinition) {
      string column_name = column_node->child_->val_;

      pSyntaxNode type_node = column_node->child_->next_;
      TypeId type_id;
      uint32_t length = 0;

      if (strcmp(type_node->val_, "int") == 0) {
        type_id = TypeId::kTypeInt;
      } else if (strcmp(type_node->val_, "float") == 0) {
        type_id = TypeId::kTypeFloat;
      } else if (strcmp(type_node->val_, "char") == 0) {
        type_id = TypeId::kTypeChar;
        if (type_node->child_ != nullptr && type_node->child_->type_ == kNodeNumber) {
          int char_length = atoi(type_node->child_->val_);
          if (char_length <= 0) {
            cout << "Invalid char length" << endl;
            for (auto col : columns) {
              delete col;
            }
            return DB_FAILED;
          }
          length = char_length;
        }
      } else {
        cout << "Unknown data type" << endl;
        for (auto col : columns) {
          delete col;
        }
        return DB_FAILED;
      }

      bool is_unique = false;
      bool is_nullable = true;
      pSyntaxNode constraint_node =type_node->next_;

      while (constraint_node != nullptr) {
        if (constraint_node->type_ == kNodeColumnType) {
          if (strcmp(constraint_node->val_, "UNIQUE") == 0) {
            is_unique = true;
          } else if (strcmp(constraint_node->val_, "NOT NULL") == 0) {
            is_nullable = false;
          } else if (strcmp(constraint_node->val_, "NULL") == 0) {
            is_nullable = true;
          }
        } else if (constraint_node->type_ == kNodeIdentifier && strcmp(constraint_node->val_, "PRIMARY") == 0) {
          primary_keys.push_back(column_name);
          is_nullable = false;
        }
        constraint_node = constraint_node->next_;
      }

      Column *column = new Column(column_name, type_id, length, column_index++, is_nullable, is_unique);
      columns.push_back(column);
    }

    column_node = column_node->next_;
  }

  pSyntaxNode constraint_node = column_list->child_;
  while (constraint_node != nullptr) {
    if (constraint_node->type_ == kNodeColumnType && strcmp(constraint_node->val_, "PRIMARY KEY") == 0) {
      pSyntaxNode pk_columns = constraint_node->child_;
      while (pk_columns != nullptr) {
        if (pk_columns->type_ == kNodeIdentifier) {
          string pk_column = pk_columns->val_;
          if (find(primary_keys.begin(), primary_keys.end(), pk_column) == primary_keys.end()) {
            primary_keys.push_back(pk_column);
            for (size_t i = 0; i < columns.size(); i++) {
              if (columns[i]->GetName() == pk_column) {
                
                string name = columns[i]->GetName();
                TypeId type = columns[i]->GetType();
                uint32_t length = columns[i]->GetLength();
                uint32_t index = columns[i]->GetTableInd();
                bool unique = columns[i]->IsUnique();
                
                delete columns[i];
                columns[i] = new Column(name, type, length, index, false, unique);
                break;
              }
            }
          }
        }
        pk_columns = pk_columns->next_;
      }
    }
    constraint_node = constraint_node->next_;
  }

  TableSchema *schema = new TableSchema(columns);

  dberr_t result = dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, schema, nullptr, table_info);
  if (result != DB_SUCCESS) {
    delete schema;
    return result;
  }

  if (!primary_keys.empty()) {
    IndexInfo *index_info = nullptr;
    dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, "PRIMARY_KEY", primary_keys, nullptr, index_info, "bptree");
  }

  for (const auto &col : columns) {
    if (col->IsUnique() && std::find(primary_keys.begin(), primary_keys.end(), col->GetName()) == primary_keys.end()) {
      vector<string> unique_key = {col->GetName()};
      IndexInfo *index_info = nullptr;
      dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, "UNIQUE_" + col->GetName(), unique_key, nullptr, index_info, "bptree");
    }
  }

  cout << "Table '" << table_name << "' created." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif

  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }

  string table_name = ast->child_->val_;

  TableInfo *table_info = nullptr;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS) {
    return DB_TABLE_NOT_EXIST;
  }

  dberr_t result = dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
  if (result != DB_SUCCESS) {
    return result;
  }

  cout << "Table '" << table_name << "' dropped." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif

  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }

  vector<TableInfo *> tables;
  dberr_t result = dbs_[current_db_]->catalog_mgr_->GetTables(tables);

  if (result != DB_SUCCESS || tables.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }

  string table_header = "Table";
  string index_header = "Index";
  string column_header = "Columns";

  uint32_t table_width = table_header.length();
  uint32_t index_width = index_header.length();
  uint32_t column_width = column_header.length();

  vector<tuple<string, string, string>> index_info_list;

  for (const auto &table : tables) {
    string table_name = table->GetTableName();
    if (table_name.length() > table_width) {
      table_width = table_name.length();
    }
    
    vector<IndexInfo *> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);

    for (const auto &index : indexes) {
      string index_name = index->GetIndexName();
      if (index_name.length() > index_width) {
        index_width = index_name.length();
      }

      string column_names;
      auto index_key_schema = index->GetIndexKeySchema();
      auto table_schema = table->GetSchema();

      for (uint32_t i = 0; i < index_key_schema->GetColumnCount(); i++) {
        if (i > 0) {
          column_names += ", ";
        }
        uint32_t table_col_idx = index_key_schema->GetColumn(i)->GetTableInd();
        column_names += table_schema->GetColumn(table_col_idx)->GetName();
      }

      if (column_names.length() > column_width) {
        column_width = column_names.length();
      }

      index_info_list.emplace_back(make_tuple(table_name, index_name, column_names));
    }
  }
  table_width += 2;
  index_width += 2;
  column_width += 2;

  if (index_info_list.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }

  // 表格边框顶部
  cout << "+" << setfill('-') << setw(table_width) << ""
       << "+" << setw(index_width) << ""
       << "+" << setw(column_width) << ""
       << "+" << endl;

  // 表格标题行
  cout << "|" << setfill(' ') << setw(table_width - 1) << std::left << " " + table_header
       << "|" << setw(index_width - 1) << std::left << " " + index_header
       << "|" << setw(column_width - 1) << std::left << " " + column_header << "|" << endl;

  // 表格标题下边框
  cout << "+" << setfill('-') << setw(table_width) << "" 
       << "+" << setw(index_width) << "" 
       << "+" << setw(column_width) << "" << "+" << endl;
  
  // 表格数据行
  for (const auto &index_info : index_info_list) {
    cout << "|" << setfill(' ') << setw(table_width - 1) << std::left << " " + std::get<0>(index_info)
         << "|" << setw(index_width - 1) << std::left << " " + std::get<1>(index_info)
         << "|" << setw(column_width - 1) << std::left << " " + std::get<2>(index_info) << "|" << endl;
  }
  
  // 表格边框底部
  cout << "+" << setfill('-') << setw(table_width) << "" 
       << "+" << setw(index_width) << "" 
       << "+" << setw(column_width) << "" << "+" << endl;
  
  // 显示结果集数量
  cout << index_info_list.size() << " rows in set (0.00 sec)" << endl;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif

  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }

  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;

  TableInfo *table_info = nullptr;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS) {
    return DB_TABLE_NOT_EXIST;
  }

  string index_type = "bptree";
  pSyntaxNode type_node = ast->child_->next_->next_->next_;
  if (type_node != nullptr && type_node->type_ == kNodeIndexType) {
    index_type = type_node->val_;
  }

  vector<string> index_keys;
  pSyntaxNode key_node = ast->child_->next_->next_->child_;
  while (key_node != nullptr) {
    index_keys.push_back(key_node->val_);
    key_node = key_node->next_;
  }

  auto schema = table_info->GetSchema();
  for (const auto &key : index_keys) {
    uint32_t column_index;
    if (schema->GetColumnIndex(key, column_index) != DB_SUCCESS) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
  }

  IndexInfo *index_info = nullptr;
  dberr_t result = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, index_info, index_type);
  if (result == DB_SUCCESS) {
    cout << "Index '" << index_name << "' created on table '" << table_name << "'." << endl;
  } else if (result == DB_INDEX_ALREADY_EXIST) {
    return DB_INDEX_ALREADY_EXIST;
  } else {
    cout << "Failed to create index '" << index_name << "' on table '" << table_name << "'." << endl;
    return DB_FAILED;
  }

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif

  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }

  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;

  TableInfo *table_info = nullptr;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS) {
    return DB_TABLE_NOT_EXIST;
  }
  IndexInfo *index_info = nullptr;
  if (dbs_[current_db_]->catalog_mgr_->GetIndex(table_name, index_name, index_info) != DB_SUCCESS) {
    return DB_INDEX_NOT_FOUND;
  }

  dberr_t result = dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_name);
  if (result != DB_SUCCESS) {
    return result;
  }

  cout << "Index '" << index_name << "' dropped from table '" << table_name << "'." << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif

  string file_name = ast->child_->val_;
  if (file_name.front() == '"' && file_name.back() == '"') {
    file_name = file_name.substr(1, file_name.length() - 2);
  }

  ifstream sql_file(file_name);
  if (!sql_file.is_open()) {
    cout << "Failed to open file: " << file_name << endl;
    return DB_FAILED;
  }

  cout << "Executing SQL file: " << file_name << endl;

  stringstream buffer;
  buffer << sql_file.rdbuf();
  string sql_content = buffer.str();

  sql_file.close();

  auto start_time = std::chrono::system_clock::now();
  int success_count = 0;

  std::istringstream iss(sql_content);
  std::string line;
  std::string sql_statement;

  auto trim = [](const std::string &str) {
    auto start = str.find_first_not_of(" \t\n\r");
    auto end = str.find_last_not_of(" \t\n\r");
    return str.substr(start, end - start + 1);
  };

  while (std::getline(iss, line)) {
    line = trim(line);
    if (line.empty() || line.substr(0, 2) == "--") {
      continue; 
    }

    sql_statement += line + " ";
    
    if (line.find(";") != std::string::npos) {
      MinisqlParserInit();

      YY_BUFFER_STATE bp = yy_scan_string(sql_statement.c_str());
      if (bp == nullptr) {
        cout << "Failed to parse SQL statement: " << sql_statement << endl;
        MinisqlParserFinish();
        continue;
      }

      yy_switch_to_buffer(bp);

      yyparse();

      if (MinisqlParserGetError()) {
        cout << "Error in SQL statement: " << MinisqlParserGetErrorMessage() << endl;
      } else {
        pSyntaxNode stmt = MinisqlGetParserRootNode();
        if (stmt != nullptr) {
          dberr_t result = Execute(stmt);
          if (result == DB_SUCCESS) {
            success_count++;
          }
        }
      }
      yy_delete_buffer(bp);
      MinisqlParserFinish();
      sql_statement.clear();
    }
  }

  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());

  std::stringstream result_stream;
  ResultWriter writer(result_stream);
  writer.EndInformation(success_count, duration_time, false);
  cout << result_stream.str();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif

  for (auto &db_pair : dbs_) {
    delete db_pair.second;
  }
  dbs_.clear();
  current_db_ = "";
  cout << "Bye." << endl;
  
  return DB_SUCCESS;
}
