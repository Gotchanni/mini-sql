#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t tot = 0;
  uint32_t field_count = fields_.size();
  memcpy(buf + tot, &field_count, sizeof(field_count));
  tot += sizeof(field_count);

  uint32_t bitmap_size = (field_count + 7) / 8;
  char *bitmap = buf+tot;
  memset(bitmap, 0, bitmap_size);
  tot += bitmap_size;
  for (uint32_t i = 0; i < field_count; ++i) {
    if (fields_[i]->IsNull()) {
      bitmap[i / 8] |= (1 << (i % 8));
    }
    else{
      tot+= fields_[i]->SerializeTo(buf + tot);
    }
  }
  return tot;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t tot = 0;
  uint32_t field_count;
  memcpy(&field_count, buf + tot, sizeof(field_count));
  tot += sizeof(field_count);

  char *bitmap = buf + tot;
  uint32_t bitmap_size = (field_count + 7) / 8;
  memset(bitmap, 0, bitmap_size);
  tot += bitmap_size;
  fields_.clear();
  fields_.resize(field_count);
  for (uint32_t i = 0; i < field_count; ++i) {
    if (bitmap[i / 8] & (1 << (i % 8))) {
      tot+=Field::DeserializeFrom(buf + tot, schema->GetColumn(i)->GetType(), &fields_[i],true);
    }
    else{
      tot+=Field::DeserializeFrom(buf + tot, schema->GetColumn(i)->GetType(), &fields_[i],false);
    }
  }
  return tot;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t tot = 0;
  tot+= sizeof(uint32_t);
  tot+= (fields_.size() + 7) / 8;
  for (uint32_t i = 0; i < fields_.size(); ++i) {
    if(!fields_[i]->IsNull()) {
      tot += fields_[i]->GetSerializedSize();
    }
  }
  return tot;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
