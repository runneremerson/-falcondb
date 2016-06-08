#ifndef FDB_T_SET_H
#define FDB_T_SET_H

#include "fdb_slice.h"
#include "fdb_context.h"
#include "fdb_object.h"




void encode_ssize_key(const char* key, size_t keylen, fdb_slice_t** pslice);
int decode_ssize_key(const char* fdbkey, size_t fdbkeylen, uint64_t* size);


void encode_sset_key(const char* key, size_t keylen, const char* member, size_t memberlen, fdb_slice_t** pslice);
int decode_sset_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t **pkey, fdb_slice_t **pmember);



int set_members(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t** rets);

int set_member_exists(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member);

int set_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* size);

int set_rem(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t* members);

int set_add(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t* members);

#endif //FDB_T_SET_H

