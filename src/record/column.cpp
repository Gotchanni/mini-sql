#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  // magic number + length of name + name + type + length + table_ind + nullable + unique
  int tot=0;
  memcpy(buf+tot,&COLUMN_MAGIC_NUM,sizeof(uint32_t));
  tot+=sizeof(COLUMN_MAGIC_NUM);
  uint32_t len = static_cast<uint32_t>(name_.length());
  memcpy(buf+tot,&len,sizeof(uint32_t));
  tot+=sizeof(uint32_t);
  memcpy(buf+tot,name_.c_str(),len);
  tot+=len;
  memcpy(buf+tot,&type_,sizeof(type_));
  tot+=sizeof(type_);
  memcpy(buf+tot,&len_,sizeof(len_));
  tot+=sizeof(len_);
  memcpy(buf+tot,&table_ind_,sizeof(table_ind_));
  tot+=sizeof(table_ind_);
  memcpy(buf+tot,&nullable_,sizeof(nullable_));
  tot+=sizeof(nullable_);
  memcpy(buf+tot,&unique_,sizeof(unique_));
  tot+=sizeof(unique_);
  return tot;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return sizeof(COLUMN_MAGIC_NUM) + sizeof(uint32_t) + name_.length() + sizeof(type_) +
         sizeof(len_) + sizeof(table_ind_) + sizeof(nullable_) + sizeof(unique_);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  int tot=0;
  uint32_t magic_num=MACH_READ_UINT32(buf+tot);
  tot+=sizeof(magic_num);
  ASSERT(magic_num==COLUMN_MAGIC_NUM,"Wrong magic number for column.");
  
  uint32_t name_len=MACH_READ_UINT32(buf+tot);
  tot+=sizeof(uint32_t);
  char name_buf[name_len+1];
  memcpy(name_buf,buf+tot,name_len);
  std::string name(name_buf,name_len/sizeof(char));
  tot+=name_len;
  TypeId type;
  memcpy(&type,buf+tot,sizeof(type));
  tot+=sizeof(type);
  uint32_t len=MACH_READ_UINT32(buf+tot);
  tot+=sizeof(len);
  uint32_t index=MACH_READ_UINT32(buf+tot);
  tot+=sizeof(index);
  bool nullable=MACH_READ_FROM(bool,buf+tot);
  tot+=sizeof(nullable);
  bool unique=MACH_READ_FROM(bool,buf+tot);
  tot+=sizeof(unique);
  if(type==TypeId::kTypeChar){
    column=new Column(name,type,len,index,nullable,unique);
  }
  else{
    column=new Column(name,type,index,nullable,unique);
  }
  return tot;
}
