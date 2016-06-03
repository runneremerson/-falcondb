#include "t_zset.h"
#include "t_keys.h"

#include "fdb_types.h"
#include "fdb_iterator.h"
#include "fdb_define.h"
#include "fdb_slice.h"
#include "fdb_context.h"
#include "fdb_bytes.h"
#include "fdb_malloc.h"
#include "util.h"

#include <limits>

#include <rocksdb/c.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

const double FDB_SCORE_MIN = std::numeric_limits<double>::min();
const double FDB_SCORE_MAX = std::numeric_limits<double>::max();

static int zset_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double score);

static int zrem_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member);

static int zget_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double* pscore);

static int zget_len(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t* length);

static int zset_incr_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t by);

static int zset_range(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t offset, uint64_t limit,
                      int reverse, fdb_iterator_t** piterator);

static int zset_scan(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double sstart, 
                     double send, uint64_t limit, int reverse, fdb_iterator_t** piterator);


void encode_zsize_key(const char* key, size_t keylen, fdb_slice_t** pslice){
    fdb_slice_t* slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_ZSIZE);
    *pslice = slice;
}

int decode_zsize_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice){
    int ret = 0;
    fdb_slice_t *key_slice = NULL;
    fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen); 

    uint8_t type = 0;
    if(fdb_bytes_read_uint8(bytes, &type)==-1){
        ret = -1;
        goto end;

    }
    if(type == FDB_DATA_TYPE_ZSIZE){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_slice_len_left(bytes, &key_slice) == -1){
        ret = -1;
        goto end;
    }
    *pslice = key_slice;
    fdb_incr_ref_count(key_slice);
    ret = 0;

end:
  fdb_bytes_destroy(bytes);
  fdb_slice_destroy(key_slice);
  return ret; 
}

void encode_zset_key(const char* key, size_t keylen, const char* member, size_t memberlen, fdb_slice_t** pslice){
    fdb_slice_t* slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, (uint8_t)keylen);
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_ZSET);
    fdb_slice_uint8_push_back(slice, '=');
    fdb_slice_string_push_back(slice, member, memberlen);
    *pslice = slice;
}

int decode_zset_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pkey, fdb_slice_t** pmember){
    int ret = 0;
    fdb_slice_t *key_slice = NULL, *member_slice = NULL;
    fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen); 

    uint8_t type = 0, len = 0;
    if(fdb_bytes_read_uint8(bytes, &type)==-1){
        ret = -1;
        goto end;
    }
    if(type != FDB_DATA_TYPE_ZSET){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_uint8(bytes, &len)==-1){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_slice_len_uint8(bytes, &key_slice)==-1){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_skip(bytes, 1)==-1){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_slice_len_left(bytes, &member_slice)==-1){
        ret = -1;
        goto end;
    }
    *pkey = key_slice;
    *pmember = member_slice;
    fdb_incr_ref_count(key_slice);
    fdb_incr_ref_count(member_slice);
    ret = 0;

end:
    fdb_bytes_destroy(bytes);
    fdb_slice_destroy(key_slice);
    fdb_slice_destroy(member_slice);
    return ret;
}


void encode_zscore_key(const char* key, size_t keylen, const char* member, size_t memberlen, double score, fdb_slice_t** pslice){
    fdb_slice_t *slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, (uint8_t)keylen);
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_ZSCORE);
    uint64_t lex_score = double_to_lex(score);
    fdb_slice_uint64_push_back(slice, lex_score);
    fdb_slice_uint8_push_back(slice, '=');
    fdb_slice_string_push_back(slice, member, memberlen);
    *pslice = slice;
}

int decode_zscore_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pkey, fdb_slice_t** pmember,  double *pscore){
    int ret = 0;
    fdb_slice_t *key_slice = NULL, *member_slice = NULL;
    fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen); 

    uint8_t type = 0;
    uint64_t lex_score = 0;
    if(fdb_bytes_read_uint8(bytes, &type)==-1){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_slice_len_uint8(bytes, &key_slice)){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_uint64(bytes, &lex_score)==-1){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_skip(bytes, 1)==-1){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_slice_len_left(bytes, &member_slice)==-1){
        ret = -1;
        goto end;
    }
    if(pkey!=NULL){
        *pkey = key_slice;
        fdb_incr_ref_count(key_slice);
    }
    if(pmember!=NULL){
        *pmember = member_slice;
        fdb_incr_ref_count(member_slice); 
    }
    if(pscore!=NULL){
        *pscore = lex_to_double(lex_score); 
    }
    ret = 0;

end:
    fdb_bytes_destroy(bytes);
    fdb_slice_destroy(key_slice);
    fdb_slice_destroy(member_slice);
    return ret;
}


static int zset_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double score){
 
    if(fdb_slice_length(key)==0 || fdb_slice_length(member)==0){
        fprintf(stderr, "%s empty key or member!\n", __func__);
        return -1;
    }
    if(fdb_slice_length(key) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s key too long!\n", __func__);
        return -1;
    }
    if(fdb_slice_length(member) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s key too long!\n", __func__);
        return -1;
    } 
    double old_score = 0.0;
    int found = zget_one(context, slot, key, member, &old_score);
    if(found == FDB_OK_NOT_EXIST || old_score != score){
        if(found != FDB_OK_NOT_EXIST){
            fdb_slice_t *slice_key1 = NULL;
            //delete zscore key
            encode_zscore_key(fdb_slice_data(key),
                              fdb_slice_length(key),
                              fdb_slice_data(member),
                              fdb_slice_length(member),
                              old_score,
                              &slice_key1);
            fdb_slot_writebatch_delete(slot, fdb_slice_data(slice_key1), fdb_slice_length(slice_key1));
            fdb_slice_destroy(slice_key1);
        }
        fdb_slice_t *slice_key2 = NULL;
        //add score key
        encode_zscore_key(fdb_slice_data(key),
                          fdb_slice_length(key),
                          fdb_slice_data(member),
                          fdb_slice_length(member),
                          score,
                          &slice_key2);
        char buf0[sizeof(uint8_t)] = {0};
        rocksdb_encode_fixed8(buf0, 1);
        fdb_slot_writebatch_put(slot, fdb_slice_data(slice_key2), fdb_slice_length(slice_key2), buf0, sizeof(uint8_t));
        fdb_slice_destroy(slice_key2);

        fdb_slice_t *slice_key0 = NULL;
        encode_zset_key(fdb_slice_data(key),
                        fdb_slice_length(key),
                        fdb_slice_data(member),
                        fdb_slice_length(member),
                        &slice_key0);
        char buf1[sizeof(uint64_t)] = {0};
        rocksdb_encode_fixed64(buf1, double_to_lex(score));
        fdb_slot_writebatch_put(slot, fdb_slice_data(slice_key0), fdb_slice_length(slice_key0), buf1, sizeof(uint64_t));
        fdb_slice_destroy(slice_key0);
        return (found==FDB_OK) ? 0 : 1;
    }
    return 0;
}

static int zget_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double* pscore){
    fdb_slice_t *slice_key = NULL;
    encode_zset_key(fdb_slice_data(key), fdb_slice_length(key), fdb_slice_data(member), fdb_slice_length(member), &slice_key);
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    char *errptr = NULL;
    size_t vallen = 0;

    char *val = rocksdb_get_cf(context->db_, readoptions, slot->handle_, fdb_slice_data(slice_key), fdb_slice_length(slice_key), &vallen, &errptr);
    fdb_slice_destroy(slice_key);
    rocksdb_readoptions_destroy(readoptions);

    int ret = 0;
    if(errptr!=NULL){
        fprintf(stderr, "%s rocksdb_get_cf fail %s.\n", __func__, errptr);
        rocksdb_free(errptr);
        ret = -1;
        goto end;
    }
    if(val != NULL){
        assert(vallen == sizeof(uint64_t));
        uint64_t lex_score = rocksdb_decode_fixed64(val);
        *pscore = lex_to_double(lex_score);
        rocksdb_free(val);
        ret = 1;
    }else{
        ret = 0;
    }

end:
    return  ret;
}

static int zrem_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member){
    if(fdb_slice_length(key) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s name too long!\n", __func__);
        return -1; 
    }
    if(fdb_slice_length(member) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s member too long!\n", __func__);
        return -1;
    }
    double old_score = 0;
    int found = zget_one(context, slot, key, member, &old_score);
    if(found != FDB_OK){
        return 0;
    }
    fdb_slice_t *slice_key1 = NULL;
    encode_zscore_key(fdb_slice_data(key),
                      fdb_slice_length(key),
                      fdb_slice_data(member),
                      fdb_slice_length(member),
                      old_score,
                      &slice_key1);
    fdb_slot_writebatch_delete(slot, fdb_slice_data(slice_key1), fdb_slice_length(slice_key1));
    fdb_slice_destroy(slice_key1);

    fdb_slice_t *slice_key0 = NULL;
    encode_zset_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(member),
                    fdb_slice_length(member),
                    &slice_key0);
    fdb_slot_writebatch_delete(slot, fdb_slice_data(slice_key0), fdb_slice_length(slice_key0));
    fdb_slice_destroy(slice_key0);
    return 1;
}

static int zget_len(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t* length){  
    fdb_slice_t *slice_key = NULL;
    encode_zsize_key(fdb_slice_data(key),
                     fdb_slice_length(key),
                     &slice_key);

    char *val, *errptr = NULL;
    size_t vallen = 0;
    rocksdb_readoptions_t* readoptions = rocksdb_readoptions_create();
    val = rocksdb_get_cf(context->db_, readoptions, slot->handle_, fdb_slice_data(slice_key), fdb_slice_length(slice_key), &vallen, &errptr);
    rocksdb_readoptions_destroy(readoptions);
    fdb_slice_destroy(slice_key);

    int ret = 0;
    if(errptr != NULL){
        fprintf(stderr, "%s rocksdb_get_cf fail %s.\n", __func__, errptr);
        rocksdb_free(errptr);
        ret = -1;
        goto end;
    }
    if(val != NULL){
        assert(vallen == sizeof(uint64_t));
        *length = rocksdb_decode_fixed64(val);
        rocksdb_free(val);
        ret = 1;
    }else{
        ret = 0;
    }

end:
    return ret;
}

static int zset_incr_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t by){
    uint64_t length = 0;
    int ret = zget_len(context, slot, key, &length);
    if(ret < 0) return -1;

    int64_t size = (int64_t)length;
    size += by;

    fdb_slice_t *slice_key = NULL;
    encode_zsize_key(fdb_slice_data(key),
                     fdb_slice_length(key),
                     &slice_key);
    if(size <= 0){
        fdb_slot_writebatch_delete(slot, fdb_slice_data(slice_key), fdb_slice_length(slice_key));
    }else{
        length = size;
        char buff[sizeof(uint64_t)] = {0};
        rocksdb_encode_fixed64(buff, length);
        fdb_slot_writebatch_put(slot, fdb_slice_data(slice_key), fdb_slice_length(slice_key), buff, sizeof(uint64_t));
    }
    fdb_slice_destroy(slice_key);
    return 0;
}

static int zset_range(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t offset, uint64_t limit,
                      int reverse, fdb_iterator_t** piterator){

    if((offset+limit) > limit){
        limit = offset + limit;
    }
    fdb_slice_t *key_start = NULL, *key_end = NULL;
    if(reverse == 0){
        double sstart = FDB_SCORE_MIN, send = FDB_SCORE_MAX;
        encode_zscore_key(fdb_slice_data(key),
                          fdb_slice_length(key),
                          NULL,
                          0,
                          sstart,
                          &key_start);
        encode_zscore_key(fdb_slice_data(key),
                          fdb_slice_length(key),
                          "\xff",
                          strlen("\xff"),
                          send,
                          &key_end);
        *piterator = fdb_iterator_create(context, slot, key_start, key_end, limit, FORWARD);  
    }else{
        double sstart = FDB_SCORE_MAX, send = FDB_SCORE_MIN;
        encode_zscore_key(fdb_slice_data(key),
                          fdb_slice_length(key),
                          "\xff",
                          strlen("\xff"),
                          sstart,
                          &key_start);
        encode_zscore_key(fdb_slice_data(key),
                          fdb_slice_length(key),
                          NULL,
                          0,
                          send,
                          &key_end);
        *piterator = fdb_iterator_create(context, slot, key_start, key_end, limit, BACKWARD); 
    }
    fdb_slice_destroy(key_start);
    fdb_slice_destroy(key_end);

    if(offset>0){
        if(fdb_iterator_skip(*piterator, offset)<0){
            return -1;
        }
    }
    return 0;
}

static int zset_scan(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double sstart, 
                     double send, uint64_t limit, int reverse, fdb_iterator_t** piterator){
    double score = 0.0;
    if(fdb_slice_length(member)==0 || zget_one(context, slot, key, member, &score)!=FDB_OK){
        score = sstart;
    }
    fdb_slice_t *key_start = NULL, *key_end = NULL;
    if(reverse == 0){
         encode_zscore_key(fdb_slice_data(key),
                          fdb_slice_length(key),
                          NULL,
                          0,
                          score,
                          &key_start);
        encode_zscore_key(fdb_slice_data(key),
                          fdb_slice_length(key),
                          "\xff",
                          strlen("\xff"),
                          send,
                          &key_end);
        *piterator = fdb_iterator_create(context, slot, key_start, key_end, limit, FORWARD);   
    }else{
        if(fdb_slice_length(member)==0){
            encode_zscore_key(fdb_slice_data(key),
                              fdb_slice_length(key),
                              "\xff",
                              strlen("\xff"),
                              score,
                              &key_start);
        }else{
            encode_zscore_key(fdb_slice_data(key),
                              fdb_slice_length(key),
                              fdb_slice_data(member),
                              fdb_slice_length(member),
                              score,
                              &key_start); 
        }
        encode_zscore_key(fdb_slice_data(key),
                          fdb_slice_length(key),
                          NULL,
                          0,
                          send,
                          &key_end);

        *piterator = fdb_iterator_create(context, slot, key_start, key_end, limit, BACKWARD);    
    }
    fdb_slice_destroy(key_start);
    fdb_slice_destroy(key_end);
    return 0;
}


int zset_add(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double score){
    int retval = keys_enc(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }

    int ret = zset_one(context, slot, key, member, score);
    if(ret >= 0){
        if(ret > 0){
            if(zset_incr_size(context, slot, key, ret) == -1){
                return FDB_ERR;
            }
        }
        char *errptr = NULL;
        fdb_slot_writebatch_commit(context, slot, &errptr);
        if(errptr != NULL){
            fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
            rocksdb_free(errptr);
            return FDB_ERR;
        }
        retval = FDB_OK;
    }else{
        retval = FDB_ERR;
    }

    return retval;
}

int zset_rem(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }
    int ret = zrem_one(context, slot, key, member);
    if(ret >= 0){
        if(ret > 0){
            if(zset_incr_size(context, slot, key, -ret)==-1){
                return FDB_ERR;
            } 
        }
        char *errptr = NULL;
        fdb_slot_writebatch_commit(context, slot, &errptr);
        if(errptr != NULL){
            fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
            rocksdb_free(errptr);
            return FDB_ERR;
        }
        retval = FDB_OK; 
    }else{
        retval = FDB_ERR;
    }
    return retval; 
}

int zset_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* size){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }

    uint64_t length = 0;
    int ret = zget_len(context, slot, key, &length);
    if(ret < 0){
        retval = FDB_ERR;
    }else if(ret >0){
        retval = FDB_OK;
        *size = (int64_t)length; 
    }
    return retval;    
}

int zset_score(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double* pscore){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }

    int ret = zget_one(context, slot, key, member, pscore);
    if(ret ==1){
        retval = FDB_OK;
    }else if(ret == 0){
        retval = FDB_OK_NOT_EXIST;
    }else{
        retval = FDB_ERR;
    }
    return retval;
}

int zset_incr(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double init, double by, double* pscore){
    int retval = keys_enc(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }
    double score = init;
    int ret = zget_one(context, slot, key, member, &score);
    if(ret >=0){
        score += by;
        ret = zset_one(context, slot, key, member, score);
        if(ret >= 0){
            if(ret > 0){
                if(zset_incr_size(context, slot, key, ret) == -1){
                    return FDB_ERR;
                }
            }
            char *errptr = NULL;
            fdb_slot_writebatch_commit(context, slot, &errptr);
            if(errptr != NULL){
                fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
                rocksdb_free(errptr);
                return FDB_ERR;
            }
            retval = FDB_OK;
        }else{
            retval = FDB_ERR;
        } 
    }else {
        retval = FDB_ERR;
    }
    return retval;
}

int zset_rank(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, int reverse, int64_t* rank){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }
    fdb_iterator_t *ziterator = NULL;
    zset_range(context, slot, key, 0, INT32_MAX, reverse, &ziterator);

    fdb_slice_t *member_slice = NULL;
    int64_t _rank = 0;
    while(1){
        if(!fdb_iterator_valid(ziterator)){
            retval = FDB_OK_NOT_EXIST;
            break;
        }
        size_t klen = 0;
        const char* key = fdb_iterator_key_raw(ziterator, &klen);
        if(decode_zscore_key(key, klen, NULL, &member_slice, NULL)<0){
            retval = FDB_OK_NOT_EXIST;
            break;
        }
        if(compare_with_length(fdb_slice_data(member), fdb_slice_length(member), fdb_slice_data(member_slice), fdb_slice_length(member_slice))==0){
            retval = FDB_OK;
            *rank = _rank;
            break;
        } 
        fdb_slice_destroy(member_slice);
        if(fdb_iterator_next(ziterator)){
            retval = FDB_OK_NOT_EXIST;
            break;
        }
        ++_rank;
    }
    fdb_iterator_destroy(ziterator);
    return retval;
}

int zset_count(fdb_context_t* context, fdb_slot_t* slot, double sstart, double send, int64_t* count){
    return 0;    
}