#ifndef FDB_BYTES_H
#define FDB_BYTES_H

#include "fdb_slice.h"

#include <stddef.h>
#include <stdint.h>

typedef struct  fdb_bytes_t     fdb_bytes_t;

fdb_bytes_t* fdb_bytes_create(const char* data, size_t len);
void fdb_bytes_destroy(void* bytes);

int fdb_bytes_skip(fdb_bytes_t* bytes, size_t n);

int fdb_bytes_read_uint8(fdb_bytes_t* bytes, uint8_t* val);

int fdb_bytes_read_int16(fdb_bytes_t* bytes, int16_t* val);

int fdb_bytes_read_uint16(fdb_bytes_t* bytes, uint16_t* val);

int fdb_bytes_read_int32(fdb_bytes_t* bytes, int32_t* val);

int fdb_bytes_read_uint32(fdb_bytes_t* bytes, uint32_t* val);

int fdb_bytes_read_int64(fdb_bytes_t* bytes, int64_t* val);

int fdb_bytes_read_uint64(fdb_bytes_t* bytes, uint64_t* val);

int fdb_bytes_read_slice_len_left(fdb_bytes_t* bytes, fdb_slice_t** pslice);

int fdb_bytes_read_slice_len_uint8(fdb_bytes_t* bytes, fdb_slice_t** pslice);

int fdb_bytes_read_slice_len_uint16(fdb_bytes_t* bytes, fdb_slice_t** pslice);

#endif //FDB_BYTES_H
