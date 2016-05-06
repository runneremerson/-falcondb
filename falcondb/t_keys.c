#include "t_keys.h"

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
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_KEYS);
    *pslice =  slice;
}

void encode_dels_key(const char* key, size_t keylen, fdb_slice_t** pslice){
    fdb_slice_t* slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_DELS);
    *pslice =  slice;
}

int decode_keys_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice){
    int retval = 0;
    fdb_slice_t *slice_key = NULL;
    fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen);

    uint8_t type = rocksdb_decode_fixed8(fdbkey);
    if(type!=FDB_DATA_TYPE_KEYS){
        goto err;
    }
    if(fdb_bytes_skip(bytes, sizeof(uint8_t)) == -1 ){
        goto err;
    }
    if(fdb_bytes_read_slice_len_left(bytes, &slice_key) == -1){
        goto err;
    }
    *pslice = slice_key;
    retval = 0;
    goto end;

err:
    fdb_slice_destroy(slice_key);
    retval = -1;
    goto end; 
end: 
    fdb_bytes_destroy(bytes);
    return retval;
}

int decode_dels_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice){
    int retval = 0;
    fdb_slice_t *slice_key = NULL;
    fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen);

    size_t read = 0;
    uint8_t type = rocksdb_decode_fixed8(fdbkey + read);
    if(type!=FDB_DATA_TYPE_DELS){
        goto err;
    }
    read += sizeof(uint8_t);
    if(fdb_bytes_skip(bytes, sizeof(uint8_t) + sizeof(uint32_t)) == -1 ){
        goto err;
    }
    if(fdb_bytes_read_slice_len_left(bytes, &slice_key) == -1){
        goto err;
    }
    *pslice = slice_key;
    retval = 0;
    goto end;

err:
    fdb_slice_destroy(slice_key);
    retval = -1;
    goto end; 
end: 
    fdb_bytes_destroy(bytes);
    return retval;
}


struct keys_val_t{
    fdb_ref_t   ref_;
    uint8_t     type_;
    uint8_t     stat_;
    uint32_t    seq_;
    void*       slice_;
};

typedef struct keys_val_t  keys_val_t;

static keys_val_t* create_keys_val(){
    keys_val_t *val = (keys_val_t*)fdb_malloc(sizeof(keys_val_t));
    memset(val, 0, sizeof(keys_val_t));
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

static size_t charge_keys_val(const fdb_slice_t* key, const keys_val_t* kval){
    size_t result = fdb_slice_length(key);
    if(kval->type_ == FDB_DATA_TYPE_STRING){
        if(kval->slice_!=NULL){
            result += fdb_slice_length((fdb_slice_t*)(kval->slice_));
        }
    }
    return result;
}



static int get_keys_val(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, keys_val_t** pkval){
    int retval = 0;
    char *val = NULL, *errptr = NULL;
    size_t vallen = 0;


    //getting from lru cache
    rocksdb_cache_handle_t *handle = rocksdb_cache_lookup(slot->keys_cache_, fdb_slice_data(key), fdb_slice_length(key));
    if(handle != NULL){
        *pkval = (keys_val_t*)rocksdb_cache_value(slot->keys_cache_, handle);
        fdb_incr_ref_count(*pkval);
        retval = 1;
        goto end;
    }
  
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
        retval = -1;
        goto end;
    }
    if(val != NULL){
        size_t read = 0;
        uint8_t type = rocksdb_decode_fixed8(val + read);
        read += sizeof(uint8_t);
        uint8_t stat = rocksdb_decode_fixed8(val + read);
        read += sizeof(uint8_t);
        uint32_t seq = rocksdb_decode_fixed32(val + read);
        read += sizeof(uint32_t);
        if(vallen>=read){
            *pkval = create_keys_val();
            (*pkval)->type_ = type;
            (*pkval)->stat_ = stat;
            (*pkval)->seq_ = seq;
            if(type == FDB_DATA_TYPE_STRING){
                (*pkval)->slice_ = fdb_slice_create(val+read, vallen-read);
            }else{
                (*pkval)->slice_ = NULL;
            }
            size_t charge = charge_keys_val(key, *pkval);
            handle = rocksdb_cache_insert(slot->keys_cache_, fdb_slice_data(key), fdb_slice_length(key), *pkval, charge, deleter_for_keys_val);
            fdb_incr_ref_count(*pkval);
        }else{
            fprintf(stderr, "%s main key type %c, value len %lu error.\n", __func__, type, vallen); 
            retval = -1;
            goto end;
        }
        retval = 1;
    }else{
        retval = 0;
    }

end:
  if(handle != NULL) rocksdb_cache_release(slot->keys_cache_, handle);
  if(val != NULL) rocksdb_free(val);
  return retval;
}

static int set_keys_val(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, keys_val_t* kval){
    int retval = 0;
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

    char buff[sizeof(uint32_t)] = {0};
    rocksdb_encode_fixed8(buff, kval->type_);
    fdb_slice_t *slice_val = fdb_slice_create(buff, sizeof(uint8_t));
    rocksdb_encode_fixed8(buff, kval->stat_);
    fdb_slice_string_push_back(slice_val, buff, sizeof(uint8_t));
    rocksdb_encode_fixed32(buff, kval->seq_);
    fdb_slice_string_push_back(slice_val, buff, sizeof(uint32_t));
    if(kval->type_ == FDB_DATA_TYPE_STRING){
        if(kval->slice_!=NULL){
           fdb_slice_string_push_back(slice_val, 
                                      fdb_slice_data((fdb_slice_t*)(kval->slice_)), 
                                      fdb_slice_length((fdb_slice_t*)(kval->slice_)));;
        } 
    }

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
        retval = -1;
        goto end;
    }
    retval = 1;

end:
    if(handle!=NULL) rocksdb_cache_release(slot->keys_cache_, handle);
    return retval;
}

static int rem_keys_val(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key){
    int retval = 0;
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
        retval = -1;
        goto end;
    }
    retval = 1;

end:
  return retval;
}


int keys_set_string(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t* val){
    int retval = 0;

    keys_val_t *kval = NULL;
    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        fdb_incr_ref_count(val);
        if(kval->type_==FDB_DATA_TYPE_STRING){
            fdb_slice_destroy(kval->slice_);
        }else{
            if(kval->stat_ == FDB_KEY_STAT_NORMAL){
                //TODO mark old main key deleted
            }
            kval->seq_ += 1;
            kval->stat_ = FDB_KEY_STAT_NORMAL;
            kval->type_ = FDB_DATA_TYPE_STRING;
        }
        kval->slice_ = val;
    }else if(ret == 0){
        fdb_incr_ref_count(val);
        kval = create_keys_val();
        kval->type_ = FDB_DATA_TYPE_STRING;
        kval->stat_ = FDB_KEY_STAT_NORMAL;
        kval->seq_ = FDB_KEY_INIT_SEQ;
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

int keys_get_string(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t** pval){
    int retval = 0;
    
    keys_val_t *kval = NULL;
    if(get_keys_val(context, slot, key, &kval)!=1){
       retval = FDB_OK_NOT_EXIST; 
    }else{
        if(kval->stat_ == FDB_KEY_STAT_NORMAL){
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

    return retval; 
}



int keys_enc(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type){
    int retval = 0;
    keys_val_t *kval = NULL;

    if(type == FDB_DATA_TYPE_STRING){
        retval = FDB_ERR_WRONG_TYPE_ERROR;
        goto end;
    }
    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        if(kval->stat_ == FDB_KEY_STAT_NORMAL){
            if(kval->type_ != type){
                retval = FDB_ERR_WRONG_TYPE_ERROR;
                goto end;
            }
        }else{
            kval->stat_ = FDB_KEY_STAT_NORMAL;
            kval->type_ = type; 
            kval->seq_ += 1;
            if(set_keys_val(context, slot, key, kval)!=1){
                retval = FDB_ERR;
                goto end;
            }
        }
        fdb_slice_uint32_push_front(key, kval->seq_); 
        retval = FDB_OK;
    }else if(ret == 0){
        kval = create_keys_val();
        kval->type_ = type;
        kval->seq_ = FDB_KEY_INIT_SEQ;
        kval->stat_ = FDB_KEY_STAT_NORMAL;
        kval->slice_ = NULL;
        if(set_keys_val(context, slot, key, kval)!=1){
            retval = FDB_ERR;
        }else{
            fdb_slice_uint32_push_front(key, kval->seq_); 
            retval = FDB_OK;
        } 
    }else{
        retval = FDB_ERR;
    }

end:
    if(kval != NULL) destroy_keys_val(kval);
    return retval;
}

int keys_exs(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type){ 
    int retval = 0;
    keys_val_t *kval = NULL;

    if(type == FDB_DATA_TYPE_STRING){
        retval = FDB_ERR_WRONG_TYPE_ERROR;
        goto end;
    }
    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        if(kval->stat_ == FDB_KEY_STAT_NORMAL){
            if(kval->type_ != type){
                retval = FDB_ERR_WRONG_TYPE_ERROR;
                goto end;
            }
        }else{
            kval->stat_ = FDB_KEY_STAT_NORMAL;
            kval->type_ = type; 
            kval->seq_ += 1;
            if(set_keys_val(context, slot, key, kval)!=1){
                retval = FDB_ERR;
                goto end;
            }
        }
        fdb_slice_uint32_push_front(key, kval->seq_); 
        retval = FDB_OK;
    }else if(ret == 0){
        retval = FDB_OK_NOT_EXIST;
    }else{
        retval = FDB_ERR;
    }

end:
    if(kval != NULL) destroy_keys_val(kval);
    return retval;
}

int keys_del(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key){
    int retval = 0;

    keys_val_t *kval = NULL;
    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        if(kval->stat_!=FDB_KEY_STAT_NORMAL){
            retval = FDB_OK_NOT_EXIST;
        }else{
            if(kval->type_ == FDB_DATA_TYPE_STRING){
                if(kval->slice_!=NULL){
                    fdb_slice_destroy(kval->slice_);
                    kval->slice_ = NULL;
                }
            }else{
                //TODO mark old main key deleted
            }
            kval->stat_ = FDB_KEY_STAT_PENDING;
            if(set_keys_val(context, slot, key, kval)!=1){
                retval = FDB_ERR;
                goto end;
            }
            retval = FDB_OK;
        }
    }else if(ret == 0){
        retval = FDB_OK_NOT_EXIST;
    }else{
        retval = FDB_ERR; 
    }

end:
    if(kval!=NULL) destroy_keys_val(kval);
    return retval;
}


int keys_rem(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key){
    int retval = 0;

    int ret = rem_keys_val(context, slot, key);
    if(ret>=0){
        retval = FDB_OK;
    }
    retval = FDB_ERR; 

    return retval;  
}

int keys_get(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, uint8_t* type){
    int retval = 0;
    keys_val_t *kval = NULL;

    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        if(kval->stat_ == FDB_KEY_STAT_NORMAL){
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
    return retval; 
}

