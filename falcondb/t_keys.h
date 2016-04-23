#ifndef FDB_T_KEYS_H
#define FDB_T_KEYS_H

#include "fdb_context.h"
#include "fdb_slice.h"


void encode_meta_key(const char* key, size_t keylen, uint8_t type, fdb_slice_t** slice);

int decode_meta_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice);




//encoding sequence number into orignal key
int keys_enc(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type);

//deleting the key and the related data struct, such as zset
int keys_del(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key);

//reming the entry
int keys_rem(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key);

//getting type
int keys_get(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, uint8_t* type);

#endif //FDB_T_KEYS_H
