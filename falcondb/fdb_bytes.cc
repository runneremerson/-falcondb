#include "fdb_bytes.h"
#include "fdb_slice.h"
#include "fdb_malloc.h"

#include <rocksdb/c.h>
#include <string.h>

struct fdb_bytes_t{
  const char* data_;
  size_t length_;
};

fdb_bytes_t* fdb_bytes_create(const char* data, size_t len){
  fdb_bytes_t *bytes = (fdb_bytes_t*)fdb_malloc(sizeof(fdb_bytes_t));
  bytes->data_ = data;
  bytes->length_ = len;
  return bytes;
}

void fdb_bytes_destroy(void* bytes){
  fdb_free(bytes);
}

int fdb_bytes_skip(fdb_bytes_t* bytes, size_t n){
  if(bytes->length_ < n ){
    return -1;
  }
  bytes->length_ -= n;
  bytes->data_ += n;
  return n;
}

int fdb_bytes_read_uint8(fdb_bytes_t* bytes, uint8_t* val){
  if(bytes->length_ < sizeof(uint8_t)){
    return -1;
  }
  *val = rocksdb_decode_fixed8(bytes->data_);
  bytes->data_ += sizeof(uint8_t);
  bytes->length_ -= sizeof(uint8_t);
  return sizeof(uint8_t);
}


int fdb_bytes_read_int16(fdb_bytes_t* bytes, int16_t* val){
  if(bytes->length_ < sizeof(int16_t)){
    return -1;
  }
  *val = rocksdb_decode_fixed16(bytes->data_);
  bytes->data_ += sizeof(int16_t);
  bytes->length_ -= sizeof(int16_t);
  return sizeof(int16_t);
}


int fdb_bytes_read_uint16(fdb_bytes_t* bytes, uint16_t* val){
  if(bytes->length_ < sizeof(uint16_t)){
    return -1;
  }
  *val = rocksdb_decode_fixed16(bytes->data_);
  bytes->data_ += sizeof(uint16_t);
  bytes->length_ -= sizeof(uint16_t);
  return sizeof(uint16_t);
}

int fdb_bytes_read_int32(fdb_bytes_t* bytes, int32_t* val){
  if(bytes->length_ < sizeof(int32_t)){
    return -1;
  }
  *val = rocksdb_decode_fixed32(bytes->data_);
  bytes->data_ += sizeof(int32_t);
  bytes->length_ -= sizeof(int32_t);
  return sizeof(int32_t);
}

int fdb_bytes_read_uint32(fdb_bytes_t* bytes, uint32_t* val){
  if(bytes->length_ < sizeof(uint32_t)){
    return -1;
  }
  *val = rocksdb_decode_fixed32(bytes->data_);
  bytes->data_ += sizeof(uint32_t);
  bytes->length_ -= sizeof(uint32_t);
  return sizeof(uint32_t);
}

int fdb_bytes_read_int64(fdb_bytes_t* bytes, int64_t* val){
  if(bytes->length_ < sizeof(int64_t)){
    return -1;
  }
  *val = rocksdb_decode_fixed64(bytes->data_);
  bytes->data_ += sizeof(int64_t);
  bytes->length_ -= sizeof(int64_t);
  return sizeof(int64_t);
}


int fdb_bytes_read_uint64(fdb_bytes_t* bytes, uint64_t* val){
  if(bytes->length_ < sizeof(uint64_t)){
    return -1;
  }
  *val = rocksdb_decode_fixed64(bytes->data_);
  bytes->data_ += sizeof(uint64_t);
  bytes->length_ -= sizeof(uint64_t);
  return sizeof(uint64_t);
}

int fdb_bytes_read_slice_len_left(fdb_bytes_t* bytes, fdb_slice_t** pslice){
  int n = bytes->length_;
  *pslice = fdb_slice_create(bytes->data_, bytes->length_);
  bytes->length_ = 0;
  bytes->data_ += n;
  return n; 
}

int fdb_bytes_read_slice_len_uint8(fdb_bytes_t* bytes, fdb_slice_t** pslice){
  if(bytes->length_ < sizeof(uint8_t)){
    return -1;
  }
  uint8_t len = rocksdb_decode_fixed8(bytes->data_);
  bytes->length_ -= sizeof(uint8_t);
  bytes->data_ += sizeof(uint8_t);
  if(bytes->length_ < len){
    return -1;
  }
  *pslice = fdb_slice_create(bytes->data_, len);
  bytes->length_ -= len;
  bytes->data_ += len;
  return len+sizeof(uint8_t);
}

int fdb_bytes_read_slice_len_uint16(fdb_bytes_t* bytes, fdb_slice_t** pslice){
  if(bytes->length_ < sizeof(uint16_t)){
    return -1;
  }
  uint16_t len = rocksdb_decode_fixed16(bytes->data_);
  bytes->length_ -= sizeof(uint16_t);
  bytes->data_ += sizeof(uint16_t);
  if(bytes->length_ < len){
    return -1;
  }
  *pslice = fdb_slice_create(bytes->data_, len);
  bytes->length_ -= len;
  bytes->data_ += len;
  return len+sizeof(uint16_t);
}

