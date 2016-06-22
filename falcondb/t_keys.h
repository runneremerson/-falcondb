#ifndef FDB_T_KEYS_H
#define FDB_T_KEYS_H

#include "fdb_context.h"
#include "fdb_slice.h"


void encode_keys_key(const char* key, size_t keylen, fdb_slice_t** pslice);

void encode_dels_key(const char* key, size_t keylen, fdb_slice_t** pslice);

int decode_keys_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice);

int decode_dels_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice);

//##keys
int keys_set_string(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* val);

int keys_get_string(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t** pval);

//get the main key if not exist then adding
int keys_enc(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type);

//get the main key if not exist just let go
int keys_exs(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type);

//del the main key
int keys_del(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* count);

//rem the main key
int keys_rem(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key);

//get the main key type 
int keys_get(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t* type);

#endif //FDB_T_KEYS_H
