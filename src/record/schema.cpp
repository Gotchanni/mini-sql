#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  // magic number + length of column + &column
  uint32_t tot = 0;
  memcpy(buf + tot, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
  tot += sizeof(uint32_t);
  uint32_t column_count = static_cast<uint32_t>(columns_.size());
  memcpy(buf + tot, &column_count, sizeof(uint32_t));
  tot += sizeof(uint32_t);
  for (const auto &column : columns_) {
    tot += column->SerializeTo(buf + tot);
  }
  return tot;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t tot = 0;
  tot += sizeof(uint32_t); // magic number
  tot += sizeof(uint32_t); // length of column
  for (const auto &column : columns_) {
    tot += column->GetSerializedSize();
  }
  return tot;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t tot = 0;
  uint32_t magic_num;
  memcpy(&magic_num, buf + tot, sizeof(uint32_t));
  tot += sizeof(uint32_t);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Wrong magic number for schema.");
  
  uint32_t column_count;
  memcpy(&column_count, buf + tot, sizeof(uint32_t));
  tot += sizeof(uint32_t);

  std::vector<Column *> columns;
  columns.reserve(column_count);
  for (uint32_t i = 0; i < column_count; ++i) {
    Column *column;
    tot += Column::DeserializeFrom(buf + tot, column);
    columns.push_back(column);
  }
  schema = new Schema(columns,true);
  return tot;
}