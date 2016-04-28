#ifndef FDB_T_STRING_H
#define FDB_T_STRING_H


#include "fdb_context.h"
#include "fdb_slice.h"
#include "fdb_object.h"

#include <stdint.h>


//string
int string_set(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t* value); 

int string_setnx(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t* value);

int string_setxx(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t* value);

int string_mset(fdb_context_t* context, fdb_slot_t* slot,  fdb_array_t* kvs, fdb_array_t** rets);

int string_msetnx(fdb_context_t* context, fdb_slot_t* slot, fdb_array_t* kvs, fdb_array_t** rets);

int string_get(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t** pvalue);

int string_mget(fdb_context_t* context, fdb_slot_t* slot, fdb_array_t* keys, fdb_array_t** rets);

int string_incr(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, int64_t init, int64_t by, int64_t* val);


#endif //FDB_T_STRING_H
