#ifndef FDB_SLICE_H
#define FDB_SLICE_H


#include <stddef.h>
#include <stdint.h>



typedef struct fdb_slice_t  fdb_slice_t;


fdb_slice_t* fdb_slice_create(const char* data, size_t len);
void fdb_slice_destroy(fdb_slice_t* slice); 

void fdb_slice_push_back(fdb_slice_t* slice, const char* data, size_t len);

void fdb_slice_push_front(fdb_slice_t* slice, const char* data, size_t len);

const char* fdb_slice_data(const fdb_slice_t* slice);

size_t fdb_slice_length(const fdb_slice_t* slice);


#endif //FDB_SLICE_H
