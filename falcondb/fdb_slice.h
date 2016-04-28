#ifndef FDB_SLICE_H
#define FDB_SLICE_H


#include <stddef.h>
#include <stdint.h>



typedef struct fdb_slice_t  fdb_slice_t;

fdb_slice_t* fdb_slice_create(const char* data, size_t len);
void fdb_slice_destroy(void* slice);


void fdb_slice_incr_ref(fdb_slice_t* slice);

//push front
void fdb_slice_string_push_front(fdb_slice_t* slice, const char* str, size_t len);

void fdb_slice_uint8_push_front(fdb_slice_t* slice, uint8_t val);

void fdb_slice_uint16_push_front(fdb_slice_t* slice, uint16_t val);

void fdb_slice_uint32_push_front(fdb_slice_t* slice, uint32_t val);

void fdb_slice_uint64_push_front(fdb_slice_t* slice, uint64_t val);


//push back
void fdb_slice_string_push_back(fdb_slice_t* slice, const char* str, size_t len);

void fdb_slice_uint8_push_back(fdb_slice_t* slice, uint8_t val);

void fdb_slice_uint16_push_back(fdb_slice_t* slice, uint16_t val);

void fdb_slice_uint32_push_back(fdb_slice_t* slice, uint32_t val);

void fdb_slice_uint64_push_back(fdb_slice_t* slice, uint64_t val);


const char* fdb_slice_data(const fdb_slice_t* slice);

size_t fdb_slice_length(const fdb_slice_t* slice);


#endif //FDB_SLICE_H
