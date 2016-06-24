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
#include <math.h>
#include <stdio.h>
#include <assert.h>

const double    FDB_SCORE_MAX = std::numeric_limits<double>::max();
const double    FDB_SCORE_MIN = std::numeric_limits<double>::lowest();
const uint8_t   OPEN_ITERVAL_LEFT   = 0x01;
const uint8_t   OPEN_ITERVAL_RIGHT  = 0x02;



static int zset_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double score);

static int zrem_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member);

static int zget_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double* pscore);

static int zget_len(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t* length);

static int zset_incr_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t by);

static int zget_range(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t offset, uint64_t limit,
                      int reverse, fdb_iterator_t** piterator);

static int zget_scan(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double sstart, 
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

    //printf("%s, score=%20f\n", __func__, score);
    //printbuf(&lex_score, sizeof(uint64_t));
    //printbuf(fdb_slice_data(slice), fdb_slice_length(slice));
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
    if(fdb_bytes_read_slice_len_uint8(bytes, &key_slice)==-1){
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
    if(found == 0 || old_score != score){
        if(found != 0){
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
        return (found==1) ? 0 : 1;
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
    if(found != 1){
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

static int zget_range(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t offset, uint64_t limit,
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

static int zget_scan(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member, double sstart, 
                     double send, uint64_t limit, int reverse, fdb_iterator_t** piterator){
    double score = 0.0;
    if(fdb_slice_length(member)==0 || zget_one(context, slot, key, member, &score)!=1){
        score = sstart;
    }
    fdb_slice_t *key_start = NULL, *key_end = NULL;
    if(reverse == 0){
         encode_zscore_key(fdb_slice_data(key),
                           fdb_slice_length(key),
                           fdb_slice_data(member),
                           fdb_slice_length(member),
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


int zset_add(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t* sms, int64_t* count){
    int retval = keys_enc(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }

    if((sms->length_%2)==1){
        return FDB_ERR_WRONG_NUMBER_ARGUMENTS;
    }

    int64_t _count = 0;
    for(size_t i=0; i<sms->length_;){
        double score = fdb_array_at(sms, i++)->val_.dval_; 
        fdb_slice_t *member = (fdb_slice_t*)(fdb_array_at(sms, i++)->val_.vval_);
        int ret = zset_one(context, slot, key, member, score);
        if(ret >=0){
            if(ret > 0){
                if(zset_incr_size(context, slot, key, 1) == 0){
                    ++_count;
                }
            }
            char *errptr = NULL;
            fdb_slot_writebatch_commit(context, slot, &errptr);
            if(errptr != NULL){
                fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
                rocksdb_free(errptr);
            }
        }
    }

    *count = _count;
    return FDB_OK;
}

int zset_rem(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t* members, int64_t* count){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }

    int64_t _count = 0;
    for(size_t i=0; i<members->length_; ++i){ 
        fdb_slice_t *member = (fdb_slice_t*)(fdb_array_at(members, i)->val_.vval_);
        int ret = zrem_one(context, slot, key, member);
        if(ret >= 0){
            if(ret > 0){
                if(zset_incr_size(context, slot, key, -ret)==0){
                    ++_count;
                }
            }
            char *errptr = NULL;
            fdb_slot_writebatch_commit(context, slot, &errptr);
            if(errptr != NULL){
                fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
                rocksdb_free(errptr);
            }
        }
    }

    *count = _count;
    return FDB_OK; 
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
    }else if(ret >=0){
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
                if(zset_incr_size(context, slot, key, ret) != 0){
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
            *pscore = score;
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
    zget_range(context, slot, key, 0, INT32_MAX, reverse, &ziterator);

    fdb_slice_t *zmember = NULL;
    int64_t _rank = 0;
    while(1){
        if(!fdb_iterator_valid(ziterator)){
            retval = FDB_OK_NOT_EXIST;
            break;
        }
        size_t klen = 0;
        const char* key = fdb_iterator_key_raw(ziterator, &klen);
        if(decode_zscore_key(key, klen, NULL, &zmember, NULL)<0){
            retval = FDB_OK_NOT_EXIST;
            break;
        }
        if(compare_with_length(fdb_slice_data(member), fdb_slice_length(member), fdb_slice_data(zmember), fdb_slice_length(zmember))==0){
            retval = FDB_OK;
            *rank = _rank;
            break;
        } 
        fdb_slice_destroy(zmember);
        if(fdb_iterator_next(ziterator)){
            retval = FDB_OK_NOT_EXIST;
            break;
        }
        ++_rank;
    }
    fdb_iterator_destroy(ziterator);
    return retval;
}

int zset_count(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, double score_start, double score_end, uint8_t type, int64_t* count){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }

    if(fabs(score_start - score_end)<=0.000001 && ((type&OPEN_ITERVAL_LEFT)||(type &OPEN_ITERVAL_RIGHT)) ){
        *count = 0;
        return FDB_OK;
    }

    fdb_iterator_t *ziterator = NULL;
    zget_scan(context, slot, key, NULL, score_start, score_end, INT32_MAX, 0, &ziterator);


    int64_t _count = 0;
    if(fdb_iterator_valid(ziterator)){
        double score = 0.0;
        size_t len = 0;
        const char* fdbkey = fdb_iterator_key_raw(ziterator, &len); 
        if(decode_zscore_key(fdbkey, len, NULL, NULL, &score)==0){
            if(fabs(score_start-score)>0.000001 || !(type&OPEN_ITERVAL_LEFT)){
                _count++; 
            }
        }
    }else{
        *count = 0;
        retval = FDB_OK;
        goto end;
    }

    while(!fdb_iterator_next(ziterator)){
        double score = 0.0;
        size_t len = 0;
        const char* fdbkey = fdb_iterator_key_raw(ziterator, &len); 
        if(decode_zscore_key(fdbkey, len, NULL, NULL, &score)==0){
            if((score_end == score) && (type & OPEN_ITERVAL_RIGHT)){
                break;
            }
            _count++;
        }

    }
    *count = _count;
    retval = FDB_OK;

end:
    fdb_iterator_destroy(ziterator);
    return retval;
}

int zset_range(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int rank_start, int rank_end, int reverse, fdb_array_t** rets){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }
    uint64_t offset, limit, length = 0;
    int ret = zget_len(context, slot, key, &length); 
    if(ret != 1 && ret != 0){
        return FDB_ERR;
    }

    if(rank_start < 0){
        rank_start = rank_start + length;
    }
    if(rank_end < 0){
        rank_end = rank_end + length;
    }
    if(rank_start < 0){
        rank_start = 0;
    }
    if(rank_start > rank_end || rank_start >= (int)length){
        return FDB_OK_RANGE_HAVE_NONE;
    }
    if(rank_end >= (int)length){
        rank_end = (int)(length - 1);
        limit = rank_end - rank_start + 10;
    }else{
        limit = rank_end - rank_start + 1;
    }
    offset = rank_start;
    fdb_iterator_t *ziterator = NULL;
    zget_range(context, slot, key, offset, limit, reverse, &ziterator);
    fdb_array_t *_rets = fdb_array_create(8);
    while(1){
        if(!fdb_iterator_valid(ziterator)){
            break;
        }
        double score = 0.0;
        size_t len = 0;
        const char* fdbkey = fdb_iterator_key_raw(ziterator, &len); 
        fdb_slice_t *zmember = NULL;
        if(decode_zscore_key(fdbkey, len, NULL, &zmember, &score)==0){
            fdb_val_node_t *snode = fdb_val_node_create();
            snode->val_.dval_ = score;
            fdb_array_push_back(_rets, snode); 

            fdb_val_node_t *mnode = fdb_val_node_create();
            mnode->val_.vval_ = zmember;
            fdb_array_push_back(_rets, mnode);
        }
        if(fdb_iterator_next(ziterator)){
            break;
        }
    }

    if(_rets->length_ == 0){
        fdb_array_destroy(_rets);
        retval = FDB_OK_RANGE_HAVE_NONE;
    }else{
        *rets = _rets;
        retval = FDB_OK;
    } 
    fdb_iterator_destroy(ziterator);
    return retval;
}

int zset_scan(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, double score_start, double score_end, int reverse, uint8_t type, fdb_array_t** rets){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }

    if(fabs(score_start - score_end)<=0.000001 && ((type&OPEN_ITERVAL_LEFT)||(type &OPEN_ITERVAL_RIGHT)) ){
        return FDB_OK_RANGE_HAVE_NONE;
    }

    fdb_iterator_t *ziterator = NULL;
    zget_scan(context, slot, key, NULL, score_start, score_end, INT32_MAX, reverse, &ziterator);

    fdb_array_t *_rets = fdb_array_create(8);
    if(fdb_iterator_valid(ziterator)){
        double score = 0.0;
        size_t len = 0;
        const char* fdbkey = fdb_iterator_key_raw(ziterator, &len); 
        fdb_slice_t *zmember = NULL;
        if(decode_zscore_key(fdbkey, len, NULL, &zmember, &score)==0){
            if(fabs(score_start-score)>0.000001 || !(type&OPEN_ITERVAL_LEFT)){
                fdb_val_node_t *snode = fdb_val_node_create();
                snode->val_.dval_ = score;
                fdb_array_push_back(_rets, snode); 

                fdb_val_node_t *mnode = fdb_val_node_create();
                mnode->val_.vval_ = zmember;
                fdb_array_push_back(_rets, mnode);
            }
        }
    }else{
        retval = FDB_OK_RANGE_HAVE_NONE;
        goto end;
    }

    while(1){
        if(fdb_iterator_next(ziterator)){
            break;
        }
        if(!fdb_iterator_valid(ziterator)){
            break;
        }
        double score = 0.0;
        size_t len = 0;
        const char* fdbkey = fdb_iterator_key_raw(ziterator, &len); 
        fdb_slice_t *zmember = NULL;
        if(decode_zscore_key(fdbkey, len, NULL, &zmember, &score)==0){
            if((score_end == score) && (type & OPEN_ITERVAL_RIGHT)){
                fdb_slice_destroy(zmember);
                break;
            }
            fdb_val_node_t *snode = fdb_val_node_create();
            snode->val_.dval_ = score;
            fdb_array_push_back(_rets, snode); 

            fdb_val_node_t *mnode = fdb_val_node_create();
            mnode->val_.vval_ = zmember;
            fdb_array_push_back(_rets, mnode);
        }
    }

end:
    if(_rets->length_ == 0){
        fdb_array_destroy(_rets);
        retval = FDB_OK_RANGE_HAVE_NONE;
    }else{
        *rets = _rets;
        retval = FDB_OK;
    } 
    fdb_iterator_destroy(ziterator);
    return retval; 
}


int zset_rem_range_by_rank(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int rank_start, int rank_end, int64_t* count){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }
    uint64_t offset, limit, length = 0;
    int ret = zget_len(context, slot, key, &length); 
    if(ret != 1 && ret != 0){
        return FDB_ERR;
    }

    if(rank_start < 0){
        rank_start = rank_start + length;
    }
    if(rank_end < 0){
        rank_end = rank_end + length;
    }
    if(rank_start < 0){
        rank_start = 0;
    }
    if(rank_start > rank_end || rank_start >= (int)length){
        return FDB_OK_RANGE_HAVE_NONE;
    }
    if(rank_end >= (int)length){
        rank_end = (int)(length - 1);
        limit = rank_end - rank_start + 10;
    }else{
        limit = rank_end - rank_start + 1;
    }
    offset = rank_start;
    fdb_iterator_t *ziterator = NULL;
    zget_range(context, slot, key, offset, limit, 0, &ziterator); 

    *count = 0;
    do{
        if((*count)==limit){
            break;
        }
        if(!fdb_iterator_valid(ziterator)){
            break;
        }
        size_t len = 0;
        const char* fdbkey = fdb_iterator_key_raw(ziterator, &len); 
        fdb_slice_t *zmember = NULL;
        if(decode_zscore_key(fdbkey, len, NULL, &zmember, NULL)==0){
            int ret = zrem_one(context, slot, key, zmember);
            if(ret >= 0){
                if(ret > 0){
                    if(zset_incr_size(context, slot, key, -ret)==0){
                        *count += 1;
                    }
                }
                char *errptr = NULL;
                fdb_slot_writebatch_commit(context, slot, &errptr);
                if(errptr != NULL){
                    fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
                    rocksdb_free(errptr);
                }
            }
            fdb_slice_destroy(zmember);
        } 
    }while(!fdb_iterator_next(ziterator));

    fdb_iterator_destroy(ziterator);
    return FDB_OK;
}


int zset_rem_range_by_score(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, double score_start, double score_end, uint8_t type, int64_t* count){ 
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_ZSET);
    if(retval != FDB_OK){
        return retval;
    }

    if(fabs(score_start - score_end)<=0.000001 && ((type&OPEN_ITERVAL_LEFT)||(type &OPEN_ITERVAL_RIGHT)) ){
        return FDB_OK_RANGE_HAVE_NONE;
    }

    fdb_iterator_t *ziterator = NULL;
    zget_scan(context, slot, key, NULL, score_start, score_end, INT32_MAX, 0, &ziterator);

    *count = 0;
    if(fdb_iterator_valid(ziterator)){
        double score = 0.0;
        size_t len = 0;
        const char* fdbkey = fdb_iterator_key_raw(ziterator, &len); 
        fdb_slice_t *zmember = NULL;
        if(decode_zscore_key(fdbkey, len, NULL, &zmember, &score)==0){
            if(fabs(score_start-score)>0.000001 || !(type&OPEN_ITERVAL_LEFT)){
                int ret = zrem_one(context, slot, key, zmember);
                if(ret >= 0){
                    if(ret > 0){
                        if(zset_incr_size(context, slot, key, -ret)==0){
                            *count += 1;
                        }
                    }
                    char *errptr = NULL;
                    fdb_slot_writebatch_commit(context, slot, &errptr);
                    if(errptr != NULL){
                        fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
                        rocksdb_free(errptr);
                    }
                }
            }
            fdb_slice_destroy(zmember);
        }
    }else{
        retval = FDB_OK_RANGE_HAVE_NONE;
        goto end;
    }

    while(1){
        if(fdb_iterator_next(ziterator)){
            break;
        }
        if(!fdb_iterator_valid(ziterator)){
            break;
        }
        double score = 0.0;
        size_t len = 0;
        const char* fdbkey = fdb_iterator_key_raw(ziterator, &len); 
        fdb_slice_t *zmember = NULL;
        if(decode_zscore_key(fdbkey, len, NULL, &zmember, &score)==0){
            if((score_end != score) || !(type & OPEN_ITERVAL_RIGHT)){
                int ret = zrem_one(context, slot, key, zmember);
                if(ret >= 0){
                    if(ret > 0){
                        if(zset_incr_size(context, slot, key, -ret)==0){
                            *count += 1;
                        }
                    }
                    char *errptr = NULL;
                    fdb_slot_writebatch_commit(context, slot, &errptr);
                    if(errptr != NULL){
                        fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
                        rocksdb_free(errptr);
                    }
                }
            }
            fdb_slice_destroy(zmember);
            if(score_end == score && (type & OPEN_ITERVAL_RIGHT)){
                break;
            }
        }
    }

end:
    fdb_iterator_destroy(ziterator);
    return FDB_OK; 
}
