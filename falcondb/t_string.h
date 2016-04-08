#ifndef FDB_T_STRING_H
#define FDB_T_STRING_H


#include "fdb_context.h"
#include "fdb_slice.h"


void encode_kv_key(const char* key, size_t keylen, fdb_slice_t** pslice);

int decode_kv_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice);


int string_set(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, const fdb_slice_t* value); 

int string_setnx(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, const fdb_slice_t* value);

int string_setxx(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, const fdb_slice_t* value);

int string_get(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t** pvalue);

int string_del(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key);

int string_incr(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, int64_t init, int64_t by, int64_t* val);

#endif //FDB_T_STRING_H
