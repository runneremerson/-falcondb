#include "t_keys.h"

#include "util.h"
#include "fdb_types.h"
#include "fdb_define.h"
#include "fdb_slice.h"
#include "fdb_context.h"
#include "fdb_bytes.h"
#include "fdb_malloc.h"

#include <rocksdb/c.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>


void encode_keys_key(const char* key, size_t keylen, fdb_slice_t** pslice){
    fdb_slice_t* slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, '+');
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_KEYS);
    *pslice =  slice;
}

void encode_dels_key(const char* key, size_t keylen, fdb_slice_t** pslice){
    fdb_slice_t* slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, '-');
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_DELS);
    *pslice =  slice;
}

int decode_keys_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice){
    int ret = 0;
    fdb_slice_t *slice_key = NULL;
    fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen);

    uint8_t type = 0;
    if(fdb_bytes_read_uint8(bytes, &type)==-1){
        ret = -1;
        goto err;
    }
    if(type != FDB_DATA_TYPE_KEYS){
        ret = -1;
        goto err;
    }
    if(fdb_bytes_skip(bytes, 1)==-1){
        ret = -1;
        goto err;
    }
    if(fdb_bytes_read_slice_len_left(bytes, &slice_key) == -1){
        ret = -1;
        goto err;
    }
    *pslice = slice_key;
    ret = 0;
    goto end;

err:
    fdb_slice_destroy(slice_key);
    ret = -1;
end: 
    fdb_bytes_destroy(bytes);
    return ret;
}

int decode_dels_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice){
    int ret = 0;
    fdb_slice_t *slice_key = NULL;
    fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen);

    uint8_t type = 0;
    if(fdb_bytes_read_uint8(bytes, &type)==-1){
        ret = -1;
        goto end;
    }
    if(type != FDB_DATA_TYPE_DELS){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_skip(bytes, 1)==-1){
        ret = -1;
        goto err;
    }
    if(fdb_bytes_read_slice_len_left(bytes, &slice_key) == -1){
        ret = -1;
        goto err;
    }
    *pslice = slice_key;
    ret = 0;
    goto end;

err:
    fdb_slice_destroy(slice_key);
    ret = -1;
end: 
    fdb_bytes_destroy(bytes);
    return ret;
}


struct keys_val_t{
    fdb_ref_t   ref_;
    uint8_t     type_;
    uint8_t     stat_;
    uint32_t    seq_;
    int64_t     ts_;
    void*       slice_;
};

typedef struct keys_val_t  keys_val_t;

static keys_val_t* create_keys_val(){
    keys_val_t *val = (keys_val_t*)fdb_malloc(sizeof(keys_val_t));
    memset(val, 0, sizeof(keys_val_t));
    val->ts_ = 0;
    val->ref_.refcnt_ = 1;
    return val;
}

static void destroy_keys_val(keys_val_t* kval){
    if(kval!=NULL){
        kval->ref_.refcnt_ -= 1;
        if(kval->ref_.refcnt_ == 0){
            if(kval->type_ == FDB_DATA_TYPE_STRING){
                fdb_slice_destroy(kval->slice_);
            }
            fdb_free(kval);
        }
    }
}


static void deleter_for_keys_val(const char* key, size_t keylen, void* val){
    destroy_keys_val((keys_val_t*)val);
}

static size_t charge_keys_val(fdb_slice_t* key, const keys_val_t* kval){
    size_t result = fdb_slice_length(key);
    if(kval->type_ == FDB_DATA_TYPE_STRING){
        if(kval->slice_!=NULL){
            result += fdb_slice_length((fdb_slice_t*)(kval->slice_));
        }
    }
    return result;
}

static int decode_keys_val(const char* val, size_t vallen, keys_val_t** pkval){
    int ret = 0;
    keys_val_t *kval = create_keys_val();
    fdb_bytes_t* bytes = fdb_bytes_create(val, vallen);

    size_t left = 0;
    if(fdb_bytes_read_uint8(bytes, &(kval->type_))==-1){
        ret = -1;
        goto err;
    }
    if(fdb_bytes_read_uint8(bytes, &(kval->stat_))==-1){
        ret = -1;
        goto err;
    }
    if(fdb_bytes_read_uint32(bytes, &(kval->seq_))==-1){
        ret = -1;
        goto err;
    }
    if(fdb_bytes_read_int64(bytes, &(kval->ts_))==-1){
        ret = -1;
        goto err;
    }
    left = vallen - sizeof(uint8_t)*2 - sizeof(uint32_t) - sizeof(int64_t);
    if(left>=0){
        if(kval->type_ == FDB_DATA_TYPE_STRING && left>0){
            fdb_slice_t *_val = NULL;
            if(fdb_bytes_read_slice_len_left(bytes, &_val) == -1){
                ret = -1;
                goto err;
            } 
            kval->slice_ = _val; 
        }else{
            kval->slice_ = NULL;
        }
    }
    *pkval = kval;
    ret = 0;
    goto end;
    
err:
    destroy_keys_val(kval);
    ret = -1;
    
end:
    fdb_bytes_destroy(bytes);
    return ret;
}

static void encode_keys_val(keys_val_t* kval, fdb_slice_t** pslice){ 
    char buf[sizeof(uint8_t)] = {0};
    rocksdb_encode_fixed8(buf, kval->type_);
    fdb_slice_t *slice_val = fdb_slice_create(buf, sizeof(uint8_t));

    fdb_slice_uint8_push_back(slice_val, kval->stat_);
    fdb_slice_uint32_push_back(slice_val, kval->seq_);
    fdb_slice_uint64_push_back(slice_val, kval->ts_);
    if(kval->type_ == FDB_DATA_TYPE_STRING){
        if(kval->slice_!=NULL){
            fdb_slice_string_push_back(slice_val, 
                                       fdb_slice_data((fdb_slice_t*)(kval->slice_)), 
                                       fdb_slice_length((fdb_slice_t*)(kval->slice_)));
        } 
    }
    *pslice = slice_val;
}



static int get_keys_val(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, keys_val_t** pkval){
    //getting from lru cache
    rocksdb_cache_handle_t *handle = rocksdb_cache_lookup(slot->keys_cache_, fdb_slice_data(key), fdb_slice_length(key));
    if(handle != NULL){
        *pkval = (keys_val_t*)rocksdb_cache_value(slot->keys_cache_, handle);
        fdb_incr_ref_count(*pkval);
        rocksdb_cache_release(slot->keys_cache_, handle);
        return 1;
    }
  
    int ret = 0;
    char *val = NULL, *errptr = NULL;
    size_t vallen = 0;
    //getting from rocksdb
    fdb_slice_t *slice_key = NULL;
    encode_keys_key(fdb_slice_data(key), fdb_slice_length(key), &slice_key);
    rocksdb_readoptions_t* readoptions = rocksdb_readoptions_create();
    val = rocksdb_get_cf(context->db_, readoptions, slot->handle_, fdb_slice_data(slice_key), fdb_slice_length(slice_key), &vallen, &errptr);
    rocksdb_readoptions_destroy(readoptions);
    fdb_slice_destroy(slice_key);
    if(errptr != NULL){
        fprintf(stderr, "%s rocksdb_get_cf fail %s.\n", __func__, errptr);
        rocksdb_free(errptr);
        return -1;
    }
    if(val != NULL){
        if(decode_keys_val(val, vallen, pkval)==0){
            size_t charge = charge_keys_val(key, *pkval);
            handle = rocksdb_cache_insert(slot->keys_cache_, fdb_slice_data(key), fdb_slice_length(key), *pkval, charge, deleter_for_keys_val);
            fdb_incr_ref_count(*pkval);
            ret = 1;
        }else{
            fprintf(stderr, "%s main key, value len %lu error.\n", __func__, vallen); 
            ret = -1;
        }
        rocksdb_free(val);
    }else{
        ret = 0;
    }

    if(handle!=NULL)rocksdb_cache_release(slot->keys_cache_, handle);
    return ret;
}

static int set_keys_val(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, keys_val_t* kval){
    int ret = 0;
    char *errptr = NULL;

    size_t charge = charge_keys_val(key, kval);

    rocksdb_cache_handle_t *handle = rocksdb_cache_insert(slot->keys_cache_, 
                                                          fdb_slice_data(key), 
                                                          fdb_slice_length(key),
                                                          kval,
                                                          charge,
                                                          deleter_for_keys_val);
    if(handle!=NULL){
        fdb_incr_ref_count(kval);
    }


    fdb_slice_t *slice_key = NULL;
    encode_keys_key(fdb_slice_data(key), fdb_slice_length(key), &slice_key);

    fdb_slice_t *slice_val = NULL;
    encode_keys_val(kval, &slice_val);


    rocksdb_writeoptions_t* writeoptions = rocksdb_writeoptions_create();
    rocksdb_put_cf(context->db_,
                   writeoptions,
                   slot->handle_,
                   fdb_slice_data(slice_key),
                   fdb_slice_length(slice_key),
                   fdb_slice_data(slice_val),
                   fdb_slice_length(slice_val),
                   &errptr);
    rocksdb_writeoptions_destroy(writeoptions);
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_val);
    if(errptr != NULL){
        fprintf(stderr, "%s rocksdb_put_cf fail %s.\n", __func__, errptr);
        rocksdb_free(errptr);
        rocksdb_cache_erase(slot->keys_cache_, fdb_slice_data(key), fdb_slice_length(key));
        ret = -1;
        goto end;
    }
    ret = 1;

end:
    if(handle!=NULL) rocksdb_cache_release(slot->keys_cache_, handle);
    return ret;
}

static int rem_keys_val(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key){
    int ret = 0;
    char *errptr = NULL;

    //deling from lru cache
    rocksdb_cache_erase(slot->keys_cache_, fdb_slice_data(key), fdb_slice_length(key));
  
    //deling from rocksdb
    fdb_slice_t *slice_key = NULL;
    encode_keys_key(fdb_slice_data(key), fdb_slice_length(key), &slice_key);
    rocksdb_writeoptions_t* writeoptions = rocksdb_writeoptions_create();
    rocksdb_delete_cf(context->db_, 
                      writeoptions, 
                      slot->handle_, 
                      fdb_slice_data(slice_key), 
                      fdb_slice_length(slice_key), 
                      &errptr);
    rocksdb_writeoptions_destroy(writeoptions);
    fdb_slice_destroy(slice_key);
    if(errptr != NULL){
        fprintf(stderr, "%s rocksdb_delete_cf fail %s.\n", __func__, errptr);
        rocksdb_free(errptr);
        ret = -1;
        goto end;
    }
    ret = 1;

end:
    return ret;
}

static int mark_key_deleted(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint32_t seq){
    char *errptr = NULL;

    fdb_slice_t *slice_key = NULL;
    encode_dels_key(fdb_slice_data(key), fdb_slice_length(key), &slice_key);

    char buf_seq[sizeof(uint32_t)] = {0};
    rocksdb_encode_fixed32(buf_seq, seq);
    fdb_slice_t *slice_val = fdb_slice_create(buf_seq, sizeof(uint32_t));

    rocksdb_writeoptions_t* writeoptions = rocksdb_writeoptions_create();
    rocksdb_put_cf(context->db_,
                   writeoptions,
                   slot->handle_,
                   fdb_slice_data(slice_key),
                   fdb_slice_length(slice_key),
                   fdb_slice_data(slice_val),
                   fdb_slice_length(slice_val),
                   &errptr);
    rocksdb_writeoptions_destroy(writeoptions);
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_val);
    if(errptr != NULL){
        fprintf(stderr, "%s rocksdb_put_cf fail %s.\n", __func__, errptr);
        rocksdb_free(errptr);
        return -1;
    }
    return 1;
}


int keys_set_string(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* val){
    int retval = 0;

    keys_val_t *kval = NULL;
    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        fdb_incr_ref_count(val);
        if(kval->type_==FDB_DATA_TYPE_STRING){
            fdb_slice_destroy(kval->slice_);
        }else{
            if(kval->stat_ == FDB_KEY_STAT_NORMAL){
                if(mark_key_deleted(context, slot, key, kval->seq_)!=1){
                    retval = FDB_ERR;
                    goto  end;
                }
            }
        }
        kval->seq_ += 1;
        kval->stat_ = FDB_KEY_STAT_NORMAL;
        kval->type_ = FDB_DATA_TYPE_STRING;
        kval->ts_ = 0;
        kval->slice_ = val;
    }else if(ret == 0){
        fdb_incr_ref_count(val);
        kval = create_keys_val();
        kval->type_ = FDB_DATA_TYPE_STRING;
        kval->stat_ = FDB_KEY_STAT_NORMAL;
        kval->seq_ = FDB_KEY_INIT_SEQ;
        kval->ts_ = 0;
        kval->slice_ = val; 
    }else{
        retval = FDB_ERR; 
        goto end;
    }
    if(kval!=NULL){
        if(set_keys_val(context, slot, key, kval)!=1){
            retval = FDB_ERR;
        }else{
            retval = FDB_OK;
        }
        goto end;
    }
    retval = FDB_OK;

end:
    if(kval!=NULL) destroy_keys_val(kval);
    return retval; 
}

int keys_get_string(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t** pval){
    int retval = 0;
    
    keys_val_t *kval = NULL;
    if(get_keys_val(context, slot, key, &kval)!=1){
       return FDB_OK_NOT_EXIST; 
    }else{
        int64_t now = (int64_t)time_ms();
        if(kval->ts_>0 && kval->ts_ <= now){
            retval = FDB_OK_NOT_EXIST;
        }else if(kval->stat_ == FDB_KEY_STAT_NORMAL){
            if(kval->type_ == FDB_DATA_TYPE_STRING){
                fdb_slice_t *sl = (fdb_slice_t*)(kval->slice_);
                fdb_incr_ref_count(sl);
                *pval = sl;
                retval = FDB_OK;
            }else{ 
                retval = FDB_ERR_WRONG_TYPE_ERROR;
            }
        }else{
            retval = FDB_OK_NOT_EXIST;
        }
    }


    if(kval != NULL)destroy_keys_val(kval);
    return retval; 
}



int keys_enc(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type){

    if(type == FDB_DATA_TYPE_STRING){
        return FDB_ERR_WRONG_TYPE_ERROR;
    }

    int retval = 0;
    keys_val_t *kval = NULL;
    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        if(kval->stat_ == FDB_KEY_STAT_NORMAL){
            if(kval->type_ != type){
                destroy_keys_val(kval);
                return FDB_ERR_WRONG_TYPE_ERROR;
            }
        }else{
            kval->stat_ = FDB_KEY_STAT_NORMAL;
            kval->type_ = type; 
            kval->seq_ += 1;
            kval->ts_ = 0;
            if(set_keys_val(context, slot, key, kval)!=1){
                destroy_keys_val(kval);
                return FDB_ERR;
            }
        }
        fdb_slice_uint32_push_front(key, kval->seq_); 
        retval = FDB_OK;
    }else if(ret == 0){
        kval = create_keys_val();
        kval->type_ = type;
        kval->seq_ = FDB_KEY_INIT_SEQ;
        kval->stat_ = FDB_KEY_STAT_NORMAL;
        kval->ts_ = 0;
        kval->slice_ = NULL;
        if(set_keys_val(context, slot, key, kval)!=1){
            retval = FDB_ERR;
        }else{
            fdb_slice_uint32_push_front(key, kval->seq_); 
            retval = FDB_OK;
        } 
        destroy_keys_val(kval);
    }else{
        retval = FDB_ERR;
    }

    return retval;
}

int keys_exs(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type){ 
    int retval = 0;
    keys_val_t *kval = NULL;

    if(type == FDB_DATA_TYPE_STRING){
        return  FDB_ERR_WRONG_TYPE_ERROR;
    }
    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        int64_t now = (int64_t)time_ms();
        if(kval->ts_>0 && kval->ts_ <= now){
            retval = FDB_OK_NOT_EXIST;
        }else if(kval->stat_ == FDB_KEY_STAT_NORMAL){
            if(kval->type_ != type){
                retval = FDB_ERR_WRONG_TYPE_ERROR;
            }else{
                fdb_slice_uint32_push_front(key, kval->seq_); 
                retval = FDB_OK;
            }
        }else{
            retval = FDB_OK_NOT_EXIST;
        }
    }else if(ret == 0){
        retval = FDB_OK_NOT_EXIST;
    }else{
        retval = FDB_ERR;
    }

    if(kval != NULL)destroy_keys_val(kval);
    return retval;
}

int keys_del(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* count){
    int retval = 0;

    keys_val_t *kval = NULL;
    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){ 
        int64_t now = (int64_t)time_ms();
        if((kval->ts_>0 && kval->ts_ <= now) || kval->stat_==FDB_KEY_STAT_PENDING){
            *count = 0; 
        }else{
            *count = 1;
        }
        if((kval->ts_>now || kval->ts_==0)&& kval->stat_ == FDB_KEY_STAT_NORMAL){
            if(kval->type_ == FDB_DATA_TYPE_STRING){
                if(kval->slice_!=NULL){
                    fdb_slice_destroy(kval->slice_);
                    kval->slice_ = NULL;
                }
                if(rem_keys_val(context, slot, key)!=1){
                    retval = FDB_ERR;
                    goto end;
                }
            }else{
                if(mark_key_deleted(context, slot, key,  kval->seq_)!=1){
                    retval = FDB_ERR;
                    goto end;
                }
                kval->stat_ = FDB_KEY_STAT_PENDING;
                kval->ts_ = 0;
                if(set_keys_val(context, slot, key, kval)!=1){
                    retval = FDB_ERR;
                    goto end;
                }    
            }
        }
        retval = FDB_OK;
    }else if(ret == 0){
        retval = FDB_OK;
        *count = 0;
    }else{
        retval = FDB_ERR; 
    }

end:
    if(kval!=NULL) destroy_keys_val(kval);
    return retval;
}


int keys_rem(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key){
    int retval = 0;

    int ret = rem_keys_val(context, slot, key);
    if(ret>=0){
        retval = FDB_OK;
    }
    retval = FDB_ERR; 

    return retval;  
}

int keys_clr(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* count){ 
    int retval = 0;

    keys_val_t *kval = NULL;
    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){ 
        int64_t now = (int64_t)time_ms();
        if(kval->ts_> 0 && kval->ts_<=now && kval->stat_==FDB_KEY_STAT_NORMAL){
            kval->ts_ = 0;
            kval->stat_ = FDB_KEY_STAT_PENDING;
            if(set_keys_val(context, slot, key, kval)!=1){
                retval = FDB_ERR;
                goto end;
            }
            *count = 1;
        }
        retval = FDB_OK;
    }else if(ret == 0){
        retval = FDB_OK;
        *count = 0;
    }else{
        retval = FDB_ERR; 
    }

end:
    if(kval!=NULL) destroy_keys_val(kval);
    return retval;
}

int keys_get(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t* type){
    int retval = 0;
    keys_val_t *kval = NULL;

    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        int64_t now = (int64_t)time_ms();
        if(kval->ts_>0 && kval->ts_<= now){
            retval = FDB_OK_NOT_EXIST;
        }else if(kval->stat_ == FDB_KEY_STAT_NORMAL){
            *type = kval->type_;
            retval = FDB_OK; 
        }else{
            retval = FDB_OK_NOT_EXIST;
        }
    }else if(ret == 0){
        retval = FDB_OK_NOT_EXIST;
    }else{
        retval = FDB_ERR;
    }

    if(kval!=NULL) destroy_keys_val(kval);
    return retval; 
}


int keys_pexpire_at(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t ts, int64_t* count){ 
    int retval = 0;
    keys_val_t *kval = NULL;

    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        int64_t now = (int64_t)time_ms();
        if(kval->ts_>0 && kval->ts_<= now){
            retval = FDB_OK; 
            *count = 0;
        }else if(kval->stat_ == FDB_KEY_STAT_NORMAL){
            *count = 1;
            retval = FDB_OK;
            kval->ts_ = ts;
            if(set_keys_val(context, slot, key, kval)!=1){
                retval = FDB_ERR;
            } 
        }else{
            retval = FDB_OK;
            *count = 0;
        }
    }else if(ret == 0){
        retval = FDB_OK;
        *count = 0;
    }else{
        retval = FDB_ERR;
    }

    if(kval!=NULL) destroy_keys_val(kval);
    return retval; 
}


int keys_pexpire_left(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* left){
    int retval = 0;
    keys_val_t *kval = NULL;

    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        int64_t now = (int64_t)time_ms();
        if(kval->ts_>0 && kval->ts_<= now){
            retval = FDB_OK;
            *left = -1;
        }else if(kval->stat_ == FDB_KEY_STAT_NORMAL){
            retval = FDB_OK;
            if(kval->ts_==0){
                *left = -2;
            }else{
                *left = (kval->ts_ - now);
            }
        }else{
            retval = FDB_OK;
            *left = -1;
        }
    }else if(ret == 0){
        retval = FDB_OK;
        *left = -1;
    }else{
        retval = FDB_ERR;
    }

    if(kval!=NULL) destroy_keys_val(kval);
    return retval; 
}


int keys_pexpire_persist(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* count){ 
    int retval = 0;
    keys_val_t *kval = NULL;

    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        int64_t now = (int64_t)time_ms();
        if(kval->ts_>0 && kval->ts_<= now){
            retval = FDB_OK;
            *count = 0;
        }else if( kval->stat_ == FDB_KEY_STAT_NORMAL){
            retval = FDB_OK;
            if(kval->ts_==0){
                *count = 0;
            }else{
                *count = 1;
            }
            kval->ts_ = 0;
            if(set_keys_val(context, slot, key, kval)!=1){
                retval = FDB_ERR;
            } 
        }else{
            retval = FDB_OK;
            *count = 0;
        }
    }else if(ret == 0){
        retval = FDB_OK;
        *count = 0;
    }else{
        retval = FDB_ERR;
    }

    if(kval!=NULL) destroy_keys_val(kval);
    return retval; 
}


int keys_self_traversal_create(fdb_context_t* context, fdb_slot_t* slot, fdb_iterator_t** iter, uint64_t limit){
    fdb_slice_t *slice_start, *slice_end = NULL;

    encode_keys_key(NULL, 0, &slice_start);
    encode_keys_key(NULL, 0, &slice_end);
    fdb_slice_string_push_back(slice_end, "\xff", strlen("\xff"));
    *iter = fdb_iterator_create(context, slot, slice_start, slice_end, limit, FORWARD);
    fdb_slice_destroy(slice_start);
    fdb_slice_destroy(slice_end);
    return 0;
}

void keys_self_traversal_destroy(fdb_iterator_t* iter){
    fdb_iterator_destroy(iter);
}

int keys_self_traversal_work(fdb_iterator_t* iter, fdb_array_t **rets, uint64_t max){
    fdb_array_t *array = fdb_array_create(32);
    
    int64_t now = (int64_t)time_ms();
    uint64_t _count = 0;
    do{ 
        if(max==_count) break;
        size_t rklen = 0, rvlen = 0;
        const char* rkey = fdb_iterator_key_raw(iter, &rklen);
        const char* rval = fdb_iterator_val_raw(iter, &rvlen);
        keys_val_t* kval = NULL;
        if(decode_keys_val(rval, rvlen, &kval)==0){
            if(kval->ts_>0 && kval->ts_<=now && kval->stat_ == FDB_KEY_STAT_NORMAL){
                fdb_slice_t *_key = NULL;
                if(decode_keys_key(rkey, rklen, &_key)==0){
                    fdb_val_node_t* knode = fdb_val_node_create();            
                    knode->retval_ = FDB_OK;
                    knode->val_.vval_ = _key;
                    fdb_array_push_back(array, knode); 
                    ++_count;
                }
            }
            destroy_keys_val(kval);
        }
    }while(!fdb_iterator_next(iter));

    if(array->length_ > 0){
        *rets = array;
    }else{
        fdb_array_destroy(array);
    }
    return (int)_count;
}

