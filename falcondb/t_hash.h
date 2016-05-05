#ifndef FDB_T_HASH_H
#define FDB_T_HASH_H

#include "fdb_slice.h"
#include "fdb_context.h"
#include "fdb_object.h"

#include <stdint.h>


void encode_hsize_key(const char* key, size_t keylen, fdb_slice_t** pslice);
int decode_hsize_key(const char* fdbkey, size_t fdbkeylen, uint64_t* size);


void encode_hash_key(const char* key, size_t keylen, const char* field, size_t fieldlen, fdb_slice_t** pslice);
int decode_hash_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t **pslice_key, fdb_slice_t **pslice_field);



int hash_get(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, const fdb_slice_t* field, fdb_slice_t** pslice);

int hash_getall(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_list_t** pfvs);

int hash_mget(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t* fields, fdb_array_t** pvals);

int hash_set(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, const fdb_slice_t* field, const fdb_slice_t* value);

int hash_setnx(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, const fdb_slice_t* field, const fdb_slice_t* value);

int hash_exists(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, const fdb_slice_t* field);

int hash_length(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t* length);

int hash_incr(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, const fdb_slice_t* field, int64_t init, int64_t by, int64_t* val);

int hash_del(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, const fdb_slice_t* field);

#endif //FDB_T_HASH_H
