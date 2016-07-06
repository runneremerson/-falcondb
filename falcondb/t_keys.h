#ifndef FDB_T_KEYS_H
#define FDB_T_KEYS_H

#include "fdb_context.h"
#include "fdb_slice.h"
#include "fdb_object.h"
#include "fdb_iterator.h"

void encode_keys_key(const char* key, size_t keylen, fdb_slice_t** pslice);

void encode_dels_key(const char* key, size_t keylen, fdb_slice_t** pslice);

int decode_keys_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice);

int decode_dels_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice);


int keys_set_string(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* val);

int keys_get_string(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t** pval);

//get the main key if not exist then adding
int keys_enc(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type);

//get the main key if not exist just let go
int keys_exs(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type);

//marking the main key as deleted 
int keys_del(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* count);

//removing the main key roughly
int keys_rem(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key);

//marking expired main key as deleted 
int keys_clr(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* count);

int keys_get(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t* type);

int keys_pexpire_at(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t ts, int64_t* count);

int keys_pexpire_left(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* left);

int keys_pexpire_persist(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* count);

//keys traversal
void keys_self_traversal_create(fdb_context_t* context, fdb_slot_t* slot, fdb_iterator_t** iter);

void keys_self_traversal_destroy(fdb_iterator_t* iter);

int keys_self_traversal_work(fdb_context_t* context, fdb_slot_t* slot, fdb_array_t **rets, fdb_iterator_t* iter, int64_t limit);

//dels traversal



#endif //FDB_T_KEYS_H
