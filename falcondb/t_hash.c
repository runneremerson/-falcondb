#include "t_hash.h"
#include "t_keys.h"

#include "fdb_iterator.h"
#include "fdb_define.h"
#include "fdb_slice.h"
#include "fdb_context.h"
#include "fdb_bytes.h"
#include "fdb_malloc.h"
#include "util.h"

#include <rocksdb/c.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

static int hget_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field, fdb_slice_t** value); 

static int hlen_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t* length);

static int hset_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field, fdb_slice_t* value); 

static int hdel_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field);

static int hash_incr_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t by);

static int hash_scan(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* fstart, fdb_slice_t* fend, 
                     uint64_t limit, int reverse, fdb_iterator_t** piterator);


void encode_hsize_key(const char* key, size_t keylen, fdb_slice_t** pslice){
    fdb_slice_t* slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_HSIZE);
    *pslice = slice; 
    fdb_incr_ref_count(slice);
    fdb_slice_destroy(slice);
}


int decode_hsize_key(const char* fdbkey, size_t fdbkeylen, uint64_t* size){ 
    int retval = 0;
    fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen);

    uint8_t type = 0;
    if(fdb_bytes_read_uint8(bytes, &type)==-1){
        retval = -1;
        goto end;
    }
    if(type != FDB_DATA_TYPE_HSIZE){
        retval = -1;
        goto end;
    }
    if(fdb_bytes_skip(bytes, 1)==-1){
        retval = -1;
        goto end;
    }
    if(fdb_bytes_read_uint64(bytes, size)==-1){
        retval = -1;
        goto end; 
    }
    retval = 0;


end:
    fdb_bytes_destroy(bytes);
    return retval;
}

void encode_hash_key(const char* key, size_t keylen, const char* field, size_t fieldlen, fdb_slice_t** pslice){ 
    fdb_slice_t *slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, (uint8_t)keylen);
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_HASH);
    fdb_slice_uint8_push_back(slice, '=');
    fdb_slice_string_push_back(slice, field, fieldlen);
    *pslice = slice;
    fdb_incr_ref_count(slice);
    fdb_slice_destroy(slice);
}

int decode_hash_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t **pslice_key, fdb_slice_t **pslice_field){
    int ret = 0;
    fdb_slice_t *slice_key = NULL, *slice_field = NULL;
    fdb_bytes_t *bytes = fdb_bytes_create(fdbkey, fdbkeylen);


    uint8_t  type = 0;
    if(fdb_bytes_read_uint8(bytes, &type)==-1){
        ret = -1;
        goto end;
    }
    if(type != FDB_DATA_TYPE_HASH){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_skip(bytes, 1)==-1){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_slice_len_uint8(bytes, &slice_key)==-1){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_skip(bytes, 1)==-1){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_slice_len_left(bytes, &slice_field)==-1){
        ret = -1;
        goto end;
    }
    *pslice_key = slice_key;
    *pslice_field = slice_field;
    fdb_incr_ref_count(slice_key);
    fdb_incr_ref_count(slice_field);
    ret = 0;

end:
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_field);
    fdb_bytes_destroy(bytes);
    return ret;
}

static int hget_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field, fdb_slice_t** pslice){
    char *val = NULL, *errptr = NULL;
    size_t vallen = 0;

    fdb_slice_t* slice_key = NULL;
    encode_hash_key(fdb_slice_data(key), fdb_slice_length(key), fdb_slice_data(field), fdb_slice_length(field), &slice_key);

    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    val = rocksdb_get_cf(context->db_, readoptions, slot->handle_, fdb_slice_data(slice_key), fdb_slice_length(slice_key), &vallen, &errptr);
    rocksdb_readoptions_destroy(readoptions);
    fdb_slice_destroy(slice_key);

    int ret = 0;
    if(errptr!=NULL){
        fprintf(stderr, "%s rocksdb_get_cf fail %s.\n", __func__, errptr);
        rocksdb_free(errptr);
        ret = -1;
        goto end;
    }
    if(val!=NULL){
        *pslice= fdb_slice_create(val, vallen);
        ret = 1;
    }else{
        ret = 0;
    }

end:
    if(val != NULL){
        rocksdb_free(val); 
    }
    return ret;
}

static int hlen_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t* length){ 
    char *val = NULL, *errptr = NULL;
    size_t vallen = 0;

    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    fdb_slice_t* slice_key = NULL;
    encode_hsize_key(fdb_slice_data(key), fdb_slice_length(key), &slice_key);
    val = rocksdb_get_cf(context->db_, readoptions, slot->handle_, fdb_slice_data(slice_key), fdb_slice_length(slice_key), &vallen, &errptr);
    rocksdb_readoptions_destroy(readoptions);
    fdb_slice_destroy(slice_key);

    int ret = 0;
    if(errptr!=NULL){
        fprintf(stderr, "%s rocksdb_get_cf fail %s.\n", __func__, errptr);
        rocksdb_free(errptr);
        ret = -1;
        goto end;
    }
    if(val!=NULL){
        if(vallen < sizeof(uint64_t)){
            ret = -1;
            goto end;
        }
        *length = rocksdb_decode_fixed64(val);
        ret = 1;
    }else{
        ret = 0;
    }

end:
    if(val != NULL){
        rocksdb_free(val); 
    }
    return ret;
}

static int hset_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field, fdb_slice_t* value){

    if(fdb_slice_length(key)==0 || fdb_slice_length(field)==0){
        fprintf(stderr, "%s empty key or field!", __func__);
        return 0;
    }
    if(fdb_slice_length(key) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s name too long!", __func__);
        return -1;
    }
    if(fdb_slice_length(field) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s field too long!", __func__);
        return -1;
    }
    int ret = 0;
    fdb_slice_t *slice_key = NULL;
    fdb_slice_t *slice_val = NULL;
    if(hget_one(context, slot, key, field, &slice_val) == FDB_OK_NOT_EXIST){
        encode_hash_key(fdb_slice_data(key),
                        fdb_slice_length(key),
                        fdb_slice_data(field),
                        fdb_slice_length(field),
                        &slice_key);

        fdb_slot_writebatch_put(slot,
                                fdb_slice_data(slice_key),
                                fdb_slice_length(slice_key),
                                fdb_slice_data(value),
                                fdb_slice_length(value));
        ret = 1;
    }else{
        encode_hash_key(fdb_slice_data(key),
                        fdb_slice_length(key),
                        fdb_slice_data(field),
                        fdb_slice_length(field),
                        &slice_key);

        fdb_slot_writebatch_put(slot,
                                fdb_slice_data(slice_key),
                                fdb_slice_length(slice_key),
                                fdb_slice_data(value),
                                fdb_slice_length(value)); 
        ret = 0;
    }

    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_val);
    return ret;
}


static int hdel_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field){
    if(fdb_slice_length(key) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s key too long!", __func__);
        return -1;
    }
    if(fdb_slice_length(field) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s field too long!", __func__);
        return -1;
    }

    fdb_slice_t *slice_val = NULL;
    if(hget_one(context, slot, key, field, &slice_val) <=0){
        return 0;
    }

    fdb_slice_t *slice_key = NULL;
    encode_hash_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(field),
                    fdb_slice_length(field),
                    &slice_key);

    fdb_slot_writebatch_delete(slot,
                               fdb_slice_data(slice_key),
                               fdb_slice_length(slice_key));

    fdb_slice_destroy(slice_key);
    return 1;
}

static int hash_incr_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t by){  
  uint64_t length = 0;
  int ret = hlen_one(context, slot, key, &length);
  if(ret!=0 && ret != 1){
    return -1;
  }
  int64_t size = (int64_t)length;
  size += by;

  fdb_slice_t* slice_key = NULL;
  encode_hsize_key(fdb_slice_data(key),
                   fdb_slice_length(key),
                   &slice_key);
  if(size <= 0){
    fdb_slot_writebatch_delete(slot,
                               fdb_slice_data(slice_key),
                               fdb_slice_length(slice_key));
  }else{
    char buff[sizeof(uint64_t)] = {0};
    length = size;
    rocksdb_encode_fixed64(buff, length);
    fdb_slot_writebatch_put(slot,
                            fdb_slice_data(slice_key),
                            fdb_slice_length(slice_key),
                            buff,
                            sizeof(buff));
  }
  return 0;
}


int hash_get(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field, fdb_slice_t** pslice){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval == FDB_OK){
        int ret = hget_one(context, slot, key, field, pslice);
        if(ret == 1){
            retval = FDB_OK;
        }else if(ret == 0){
            retval = FDB_OK_NOT_EXIST;
        }else{
            retval = FDB_ERR;
        }
    }
    return  retval;
}


int hash_getall(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t** rets){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval == FDB_OK){
        fdb_iterator_t *iterator = NULL;
        if(hash_scan(context, slot, key, NULL, NULL, 20000000, 0, &iterator)!=0){
            retval = FDB_OK_RANGE_HAVE_NONE;
            goto end;
        }
        fdb_array_t *array = fdb_array_create(128);
        do{
            size_t rklen = 0, rvlen = 0;
            const char* rkey = fdb_iterator_key_raw(iterator, &rklen);
            const char* rval = fdb_iterator_val_raw(iterator, &rvlen);
            fdb_slice_t* field = NULL;
            if(decode_hash_key(rkey, rklen, NULL, &field)==0){
                fdb_val_node_t* knode = fdb_val_node_create();
                knode->retval_ = FDB_OK;
                knode->val_.vval_ = fdb_slice_create(rkey, rklen);
                fdb_array_push_back(array, knode);

                fdb_val_node_t* vnode = fdb_val_node_create();
                vnode->retval_ = FDB_OK;
                vnode->val_.vval_ = fdb_slice_create(rval, rvlen);
                fdb_array_push_back(array, vnode);
            } 
        }while(!fdb_iterator_next(iterator));
        fdb_iterator_destroy(iterator);
        *rets = array;
        retval = FDB_OK; 
    }

end:
    return retval;
}

int hash_keys(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t** rets){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval == FDB_OK){
        fdb_iterator_t *iterator = NULL;
        if(hash_scan(context, slot, key, NULL, NULL, 20000000, 0, &iterator)!=0){
            retval = FDB_OK_RANGE_HAVE_NONE;
            goto end;
        }
        fdb_array_t *array = fdb_array_create(128);
        do{
            size_t rklen = 0;
            const char* rkey = fdb_iterator_key_raw(iterator, &rklen);
            fdb_slice_t* field = NULL;
            if(decode_hash_key(rkey, rklen, NULL, &field)==0){
                fdb_val_node_t* node = fdb_val_node_create();
                node->retval_ = FDB_OK;
                node->val_.vval_ = fdb_slice_create(rkey, rklen);
                fdb_array_push_back(array, node);
            } 
        }while(!fdb_iterator_next(iterator));
        fdb_iterator_destroy(iterator);
        *rets = array;
        retval = FDB_OK; 
    } 

end:
    return retval;
}

int hash_vals(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t** rets){ 
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval == FDB_OK){
        fdb_iterator_t *iterator = NULL;
        if(hash_scan(context, slot, key, NULL, NULL, 20000000, 0, &iterator)!=0){
            retval = FDB_OK_RANGE_HAVE_NONE;
            goto end;
        }
        fdb_array_t *array = fdb_array_create(128);
        do{
            size_t rklen = 0, rvlen = 0;
            const char* rkey = fdb_iterator_key_raw(iterator, &rklen);
            const char* rval = fdb_iterator_val_raw(iterator, &rvlen);
            if(decode_hash_key(rkey, rklen, NULL, NULL)==0){
                fdb_val_node_t* vnode = fdb_val_node_create();
                vnode->retval_ = FDB_OK;
                vnode->val_.vval_ = fdb_slice_create(rval, rvlen);
                fdb_array_push_back(array, vnode);
            } 
        }while(!fdb_iterator_next(iterator));
        fdb_iterator_destroy(iterator);
        *rets = array;
        retval = FDB_OK; 
    }

end:
    return retval;
}

int hash_mget(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t* fields, fdb_array_t** rets){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval == FDB_OK){
        int len = fields->length_;
        fdb_array_t *array = fdb_array_create(len);
        for(size_t i=0; i<fields->length_; ++i){ 
            fdb_slice_t *fld = (fdb_slice_t*)(fdb_array_at(fields, i)->val_.vval_);
            fdb_slice_t *val = NULL;
            fdb_val_node_t *node = fdb_val_node_create(); 
            int ret = hget_one(context, slot, key, fld, &val);
            if(ret == 1){
                node->retval_ = FDB_OK;
                node->val_.vval_ = val;
            }else if(ret == 0){
                node->retval_ = FDB_OK_NOT_EXIST;
            }else{
                node->retval_ = FDB_ERR;
            }
            fdb_array_push_back(array, node);
        }
        retval = FDB_OK;
        *rets = array; 
    }

    return retval;
}


int hash_set(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field, fdb_slice_t* value){ 

    int retval = keys_enc(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval != FDB_OK){
        goto end;
    }

    int ret = hset_one(context, slot, key, field, value); 
    if(ret >=0){
        if(ret > 0){
            if(hash_incr_size(context, slot, key, 1) < 0){
                retval = FDB_ERR;
                goto end;
            }
        }
        char *errptr = NULL;
        fdb_slot_writebatch_commit(context, slot, &errptr);
        if(errptr != NULL){
            fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
            rocksdb_free(errptr);
            retval = FDB_ERR;
            goto  end;
        }
        retval = FDB_OK;
    }else{
        retval = FDB_ERR;
    }

end:
    return retval;
}


int hash_setnx(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field, fdb_slice_t* value){ 
    int retval = keys_enc(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval == FDB_OK){
        fdb_slice_t* slice_val = NULL;
        int ret = hget_one(context, slot, key, field, &slice_val);
        if(ret == 1){
            retval = FDB_OK_BUT_ALREADY_EXIST;
        }else if(ret == 0){
            ret = hset_one(context, slot, key, field, value);
            if(ret > 0){
                if(hash_incr_size(context, slot, key, 1) < 0){
                    retval = FDB_ERR;
                    goto end;
                }
            }
            char *errptr = NULL;
            fdb_slot_writebatch_commit(context, slot, &errptr);
            if(errptr != NULL){
                fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
                rocksdb_free(errptr);
                retval = FDB_ERR;
                goto  end;
            }
            retval = FDB_OK;
        }else{
            retval = FDB_ERR;
        }
        fdb_slice_destroy(slice_val);
    }

end:
    return retval;
}


int hash_length(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t* length){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval == FDB_OK){
        int ret =  hlen_one(context, slot, key, length);
        if(ret == 1){
            retval = FDB_OK;
        }else if(ret == 0){
            retval = FDB_OK_NOT_EXIST;
        }else{
            retval = FDB_ERR;
        }
    }
    return retval;
}


int hash_del(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field){ 
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval != FDB_OK){
        goto end;
    }
    int ret = hdel_one(context, slot, key, field);
    if(ret >=0){
        if(ret > 0){
            if(hash_incr_size(context, slot, key, -1) < 0){
                retval = FDB_ERR;
                goto end;
            }
            char *errptr = NULL;
            fdb_slot_writebatch_commit(context, slot, &errptr);
            if(errptr != NULL){
                fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", errptr, __func__);
                rocksdb_free(errptr);
                retval = FDB_ERR;
                goto  end;
            }
            retval = FDB_OK;
            goto end;
        }
        retval = FDB_OK_NOT_EXIST;
    }else{
        retval = FDB_ERR;
    }

end:
    return retval;
}

int hash_exists(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval == FDB_OK){
        fdb_slice_t *slice_val = NULL;
        int ret = hget_one(context, slot, key, field, &slice_val);
        if(ret == 1){
            retval = FDB_OK;
        }else if(ret == 0){
            retval = FDB_OK_NOT_EXIST;
        }else{
            retval = FDB_ERR;
        }
        fdb_slice_destroy(slice_val);
    }
    return retval;
}

int hash_incr(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* field, int64_t init, int64_t by, int64_t* val){
    int retval = keys_enc(context, slot, key, FDB_DATA_TYPE_HASH);
    if(retval == FDB_OK){
        fdb_slice_t *slice_val = NULL;
        int ret = hget_one(context, slot, key, field, &slice_val);
        if(ret == 1){ 
            if(fdb_slice_length(slice_val) != sizeof(int64_t)){
                retval = FDB_ERR_IS_NOT_INTEGER;
                goto end;
            }
            uint64_t _old = rocksdb_decode_fixed64(fdb_slice_data(slice_val));
            int64_t *old = (int64_t*)&_old; 
            if(is_int64_overflow(*old, by)){
                retval = FDB_ERR_INCDECR_OVERFLOW;
                goto end;
            }
            char buff[sizeof(int64_t)] = {0};
            rocksdb_encode_fixed64(buff, *old + by);
            fdb_slice_destroy(slice_val);
            slice_val = fdb_slice_create(buff, sizeof(int64_t));
        }else if(ret == 0){
            if(is_int64_overflow(init, by)){
                retval = FDB_ERR_INCDECR_OVERFLOW;
                goto end;
            } 
            char buff[sizeof(int64_t)] = {0};
            rocksdb_encode_fixed64(buff, init + by);
            fdb_slice_destroy(slice_val);
            slice_val = fdb_slice_create(buff, sizeof(int64_t));
        }else{
            retval = FDB_ERR;
            goto end;
        }
        ret = hset_one(context, slot, key, field, slice_val);
        if(ret >=0){
            if(ret > 0){
                if(hash_incr_size(context, slot, key, 1) < 0){
                    retval = FDB_ERR;
                    goto end;
                }
            }
            char *errptr = NULL;
            fdb_slot_writebatch_commit(context, slot, &errptr);
            if(errptr != NULL){
                fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", __func__, errptr);
                rocksdb_free(errptr);
                retval = FDB_ERR;
                goto  end;
            }
            retval = FDB_OK;
        }else{
            retval = FDB_ERR;
        }
    } 

end:
    return retval;
}

static int hash_scan(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* fstart, fdb_slice_t* fend, 
                     uint64_t limit, int reverse, fdb_iterator_t** piterator){

  fdb_slice_t *slice_start, *slice_end = NULL;
  if(!reverse){

    encode_hash_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(fstart),
                    fdb_slice_length(fstart),
                    &slice_start);
    encode_hash_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(fend),
                    fdb_slice_length(fend),
                    &slice_end);
    if(fdb_slice_length(fend) == 0){
      fdb_slice_string_push_back(slice_end, "\xff", strlen("\xff"));
    }

    *piterator = fdb_iterator_create(context, slot, key, slice_start, slice_end, limit, FORWARD);
  }else{
    encode_hash_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(fstart),
                    fdb_slice_length(fstart),
                    &slice_start);
    if(fdb_slice_length(fstart) == 0){
      fdb_slice_string_push_back(slice_start, "\xff", strlen("\xff"));
    }
    encode_hash_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(fend),
                    fdb_slice_length(fend),
                    &slice_end);
    *piterator = fdb_iterator_create(context, slot, key, slice_start, slice_end, limit, BACKWARD);
  }

  fdb_slice_destroy(slice_start);
  fdb_slice_destroy(slice_end);
  return 0;
}
