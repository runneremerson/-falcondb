#ifndef FDB_T_ZSET_H
#define FDB_T_ZSET_H


#include "fdb_slice.h"
#include "fdb_context.h"
#include "fdb_object.h"


void encode_zsize_key(const char* key, size_t keylen, fdb_slice_t** pslice);
int decode_zsize_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice);

void encode_zset_key(const char* key, size_t keylen, const char* member, size_t memberlen, fdb_slice_t** pslice);
int decode_zset_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t **pslice_key, fdb_slice_t **pslice_member);

void encode_zscore_key(const char* key, size_t keylen, const char* member, size_t memberlen, int64_t score, fdb_slice_t** pslice);
int decode_zscore_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice_key, fdb_slice_t** pslice_member, double* pscore);


int zset_add(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double score);

int zset_rem(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key);

int zset_rem_range_by_rank(fdb_context_t* context, fdb_slice_t* key, int rank_start, int rank_end, int64_t* count);

int zset_rem_range_by_score(fdb_context_t* context, fdb_slice_t* key, double score_start, double score_end, int64_t* count);

#endif //FDB_T_ZSET_H
