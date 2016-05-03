#include "fdb_slice.h"
#include "fdb_malloc.h"


#include <rocksdb/c.h>
#include <string.h>



struct fdb_slice_t {
    fdb_ref_t ref_;
    size_t capacity_;
    size_t start_;
    size_t length_;
    char *data_;
};


fdb_slice_t* fdb_slice_create(const char* data, size_t len){
    fdb_slice_t *slice = (fdb_slice_t*)fdb_malloc(sizeof(fdb_slice_t));
    if(len==0 || data == NULL){
        slice->length_ = 0;
        slice->capacity_ = 8 + slice->length_ + 1;
        slice->start_ = 8;
        slice->data_ = NULL;
    }else{
        slice->length_ = len;
        slice->capacity_ = 8 + slice->length_ + 1;
        slice->data_ = fdb_malloc(slice->capacity_);
        slice->start_ = 8;
        memcpy(slice->data_ + slice->start_, data, len);
    }
    
    slice->data_[slice->start_ + slice->length_] = '\0';
    slice->ref_.refcnt_ = 1;
    return slice;
}


void fdb_slice_destroy(void* slice){
  if(slice!=NULL){
    fdb_slice_t *sl = (fdb_slice_t*)slice;
    sl->ref_.refcnt_ -= 1;
    if(sl->ref_.refcnt_ == 0){
      fdb_free(sl->data_);
      fdb_free(sl);     
    }
  }
}



static size_t ensure_slice_capacity(size_t len){
  size_t capacity = len;
  if(capacity>16){
    capacity += (len>>1);
  }else{
    capacity += len;
  }
  return capacity;
}

void fdb_slice_string_push_back(fdb_slice_t* slice, const char* str, size_t len){
  if(len > 0){
    if(slice->capacity_ < (slice->start_ + slice->length_ + len + 1)){
      slice->capacity_ = ensure_slice_capacity(slice->start_ + slice->length_ + len + 1);
      slice->data_ = fdb_realloc(slice->data_, slice->capacity_);
    }
    memcpy(slice->data_ + slice->start_ + slice->length_, str, len);
    slice->length_ += len;
    slice->data_[slice->start_ + slice->length_] = '\0';
  }
}

void fdb_slice_uint8_push_back(fdb_slice_t* slice, uint8_t val){
    char buf[sizeof(uint8_t)] = {0};
    rocksdb_encode_fixed8(buf, val);
    fdb_slice_string_push_back(slice, buf, sizeof(uint8_t));
}

void fdb_slice_uint16_push_back(fdb_slice_t* slice, uint16_t val){
    char buf[sizeof(uint16_t)] = {0};
    rocksdb_encode_fixed16(buf, val);
    fdb_slice_string_push_back(slice, buf, sizeof(uint16_t));
}

void fdb_slice_uint32_push_back(fdb_slice_t* slice, uint32_t val){
    char buf[sizeof(uint32_t)] = {0};
    rocksdb_encode_fixed32(buf, val);
    fdb_slice_string_push_back(slice, buf, sizeof(uint32_t));
}

void fdb_slice_uint64_push_back(fdb_slice_t* slice, uint64_t val){
    char buf[sizeof(uint64_t)] = {0};
    rocksdb_encode_fixed64(buf, val);
    fdb_slice_string_push_back(slice, buf, sizeof(uint64_t)); 
}

void fdb_slice_string_push_front(fdb_slice_t* slice, const char* str, size_t len){
    if(len > 0){
        if(slice->start_ < len){
            slice->capacity_ = ensure_slice_capacity(8 + slice->length_ + len + 1);
            size_t old_start = slice->start_;
            size_t old_length = slice->length_;
            slice->start_ = 8;
            slice->data_ = fdb_realloc(slice->data_, slice->capacity_);
            memmove(slice->data_ + slice->start_ + len, slice->data_ + old_start, old_length);
            memcpy(slice->data_ + slice->start_, str, len);
            slice->length_ += len;
            slice->data_[slice->length_ + slice->start_] = '\0';
        }
        else{
            slice->start_ -= len;
            slice->length_ += len;
            memcpy(slice->data_+slice->start_, str, len);
        }
    }
}

void fdb_slice_uint8_push_front(fdb_slice_t* slice, uint8_t val){
    char buf[sizeof(uint8_t)] = {0};
    rocksdb_encode_fixed8(buf, val);
    fdb_slice_string_push_front(slice, buf, sizeof(uint8_t)); 
}

void fdb_slice_uint16_push_front(fdb_slice_t* slice, uint16_t val){
    char buf[sizeof(uint16_t)] = {0};
    rocksdb_encode_fixed16(buf, val);
    fdb_slice_string_push_front(slice, buf, sizeof(uint16_t)); 
}

void fdb_slice_uint32_push_front(fdb_slice_t* slice, uint32_t val){
    char buf[sizeof(uint32_t)] = {0};
    rocksdb_encode_fixed32(buf, val);
    fdb_slice_string_push_front(slice, buf, sizeof(uint32_t));
}

void fdb_slice_uint64_push_front(fdb_slice_t* slice, uint64_t val){
    char buf[sizeof(uint64_t)] = {0};
    rocksdb_encode_fixed64(buf, val);
    fdb_slice_string_push_front(slice, buf, sizeof(uint64_t)); 
}

const char* fdb_slice_data(const fdb_slice_t* slice){
  if(slice != NULL){
    return slice->data_ + slice->start_;     
  }
  return NULL;
}

size_t fdb_slice_length(const fdb_slice_t* slice){
  if(slice != NULL){
    return slice->length_;     
  }
  return 0;
}
