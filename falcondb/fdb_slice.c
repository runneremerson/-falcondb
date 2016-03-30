#include "fdb_slice.h"
#include "fdb_malloc.h"

#include <string.h>



struct fdb_slice_t {
    size_t length_;
    size_t capacity_;
    char *data_;
};




fdb_slice_t* fdb_slice_create(const char* data, size_t len){
  fdb_slice_t *slice = (fdb_slice_t*)fdb_malloc(sizeof(fdb_slice_t));
  if(len==0 || data == NULL){
    slice->data_ = NULL;
    slice->length_ = 0;
  }else{
    slice->data_ = fdb_malloc(len + 1);
    memcpy(slice->data_, data, len);
    slice->data_[len] = '\0';
    slice->length_ = len;
  }
  slice->capacity_ = slice->length_ + 1;
  return slice;
}

void fdb_slice_destroy(fdb_slice_t* slice){
  if(slice!=NULL){
    fdb_free(slice->data_);
    fdb_free(slice);
  }
}

static size_t ensure_capacity(size_t len){
  size_t capacity = len;
  if(capacity>16){
    capacity += (len>>1);
  }else{
    capacity += len;
  }
  return capacity;
}

void fdb_slice_push_back(fdb_slice_t* slice, const char* data, size_t len){
  if(len > 0){
    if(slice->capacity_ < (slice->length_ + len + 1)){
      slice->capacity_ = ensure_capacity(slice->length_ + len + 1);
      slice->data_ = fdb_realloc(slice->data_, slice->capacity_);
    }
    memcpy(slice->data_ + slice->length_, data, len);
    slice->length_ += len;
    slice->data_[slice->length_] = '\0';
  }
}


void fdb_slice_push_front(fdb_slice_t* slice, const char* data, size_t len){
  if(len > 0){
    if(slice->capacity_ < (slice->length_ + len + 1)){
      slice->capacity_ = ensure_capacity(slice->length_ + len + 1);
      slice->data_ = fdb_realloc(slice->data_, slice->capacity_);
    }
    memmove(slice->data_ + len, slice->data_, slice->length_);
    memcpy(slice->data_, data, len);
    slice->length_ += len;
    slice->data_[slice->length_] = '\0';
  }
}

const char* fdb_slice_data(const fdb_slice_t* slice){
  if(slice != NULL){
    return slice->data_;     
  }
  return NULL;
}

size_t fdb_slice_length(const fdb_slice_t* slice){
  if(slice != NULL){
    return slice->length_;     
  }
  return 0;
}
