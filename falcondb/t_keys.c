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


void encode_meta_key(const char* key, size_t keylen, uint8_t type, fdb_slice_t** pslice){
  fdb_slice_t* slice = fdb_slice_create(key, keylen);
  fdb_slice_uint8_push_front(slice, type);
  *pslice =  slice;
}

int decode_meta_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice){
  int retval = 0;
  fdb_slice_t *slice_key = NULL;
  fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen);

  uint8_t type = rocksdb_decode_fixed8(fdbkey);
  if(type!=FDB_META_TYPE_KEYS){
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

struct keys_val{
    uint8_t     type_;
    uint16_t    seq_;    
};

typedef struct keys_val  keys_val;

static keys_val* create_keys_val(uint8_t type, uint16_t seq){
    keys_val *val = (keys_val*)fdb_malloc(sizeof(keys_val));
    val->type_=  type;
    val->seq_ = seq;
    return val;
}

static void destroy_keys_val(void* val){
    fdb_free(val);
}

static void deleter_for_keys_val(const char* key, size_t keylen, void* val){
    destroy_keys_val(val);
}


static int get_keys_val(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, keys_val** pkval){
    int retval = 0;
    char *val = NULL, *errptr = NULL;
    size_t vallen = 0;


    //getting from lru cache
    rocksdb_cache_handle_t *handle = rocksdb_cache_lookup(slot->keys_cache_, fdb_slice_data(key), fdb_slice_length(key));
    if(handle != NULL){
        *pkval = (keys_val*)rocksdb_cache_value(slot->keys_cache_, handle);
        retval = 1;
        goto end;
    }
  
    //getting from rocksdb
    fdb_slice_t *slice_key = NULL;
    encode_meta_key(fdb_slice_data(key), fdb_slice_length(key), FDB_META_TYPE_KEYS, &slice_key);
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
        uint8_t type = rocksdb_decode_fixed8(val);
        uint16_t seq = rocksdb_decode_fixed16(val + sizeof(uint8_t));
        *pkval = create_keys_val(type, seq);
        retval = 1;
    }else{
        retval = 0;
    }

end:
  if(handle != NULL) rocksdb_cache_release(slot->keys_cache_, handle);
  if(val != NULL) rocksdb_free(val);
  return retval;
}

static int set_keys_val(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, keys_val* kval){
    int retval = 0;
    char *errptr = NULL;

    size_t charge = fdb_slice_length(key) + sizeof(keys_val);
    rocksdb_cache_handle_t *handle = rocksdb_cache_insert(slot->keys_cache_, 
                                                          fdb_slice_data(key), 
                                                          fdb_slice_length(key),
                                                          kval,
                                                          charge,
                                                          deleter_for_keys_val);

    fdb_slice_t *slice_key = NULL;
    encode_meta_key(fdb_slice_data(key), fdb_slice_length(key), FDB_META_TYPE_KEYS, &slice_key);
    rocksdb_writeoptions_t* writeoptions = rocksdb_writeoptions_create();
    char buff[sizeof(uint8_t) + sizeof(uint16_t)] = {0};
    char* pos = buff;
    rocksdb_encode_fixed8(pos, kval->type_);
    pos += sizeof(uint8_t);
    rocksdb_encode_fixed16(pos, kval->seq_);
    rocksdb_put_cf(context->db_,
                   writeoptions,
                   slot->handle_,
                   fdb_slice_data(slice_key),
                   fdb_slice_length(slice_key),
                   buff,
                   sizeof(uint8_t) + sizeof(uint16_t), 
                   &errptr);
    rocksdb_writeoptions_destroy(writeoptions);
    fdb_slice_destroy(slice_key);
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

//static int del_keys_val(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key){
//    int retval = 0;
//    char *errptr = NULL;
//
//    //deling from lru cache
//    rocksdb_cache_erase(slot->keys_cache_, fdb_slice_data(key), fdb_slice_length(key));
//  
//    //deling from rocksdb
//    fdb_slice_t *slice_key = NULL;
//    encode_meta_key(fdb_slice_data(key), fdb_slice_length(key), FDB_META_TYPE_KEYS, &slice_key);
//    rocksdb_writeoptions_t* writeoptions = rocksdb_writeoptions_create();
//    rocksdb_delete_cf(context->db_, 
//                      writeoptions, 
//                      slot->handle_, 
//                      fdb_slice_data(slice_key), 
//                      fdb_slice_length(slice_key), 
//                      &errptr);
//    rocksdb_writeoptions_destroy(writeoptions);
//    fdb_slice_destroy(slice_key);
//    if(errptr != NULL){
//        fprintf(stderr, "%s rocksdb_delete_cf fail %s.\n", __func__, errptr);
//        rocksdb_free(errptr);
//        retval = -1;
//        goto end;
//    }
//    retval = 1;
//
//end:
//  return retval;
//}


int keys_enc(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint8_t type){
    int retval = 0;
    keys_val *kval = NULL;

    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        fdb_slice_uint16_push_front(key, kval->seq_); 
        retval = FDB_OK;
    }else if(ret == 0){
        kval = create_keys_val(type, 0xFF01);
        if(set_keys_val(context, slot, key, kval)!=1){
            retval = FDB_ERR;
        }else{
            fdb_slice_uint16_push_front(key, kval->seq_); 
            retval = FDB_OK;
        } 
    }else{
        retval = FDB_ERR;
    }
    return retval;
}

int keys_del(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key){
    int retval = 0;
    keys_val *kval = NULL;

    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        //TODO save the old keys_val to other place.

        kval->seq_ += 1;
        if(set_keys_val(context, slot, key, kval)!=1){
            retval = FDB_ERR;
        }else{
            retval = FDB_OK;
        }
        goto end;
    }
    retval = FDB_ERR;

end:
    return retval;
}

int keys_get(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, uint8_t* type){
    int retval = 0;
    keys_val *kval = NULL;

    int ret = get_keys_val(context, slot, key, &kval);
    if(ret == 1){
        *type = kval->type_;
        retval = FDB_OK;
    }else if(ret == 0){
        retval = FDB_OK_NOT_EXIST;
    }else{
        retval = FDB_ERR;
    }
    return retval; 
}

