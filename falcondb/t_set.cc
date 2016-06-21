#include "t_set.h"
#include "t_keys.h"

#include "fdb_types.h"
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

static int sset_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member);

static int sget_len(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t* length);

static int sget_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member);

static int srem_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member);

static int set_incr_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t by);

static int sget_scan(fdb_context_t* context, fdb_slot_t *slot, fdb_slice_t* key, fdb_slice_t* mstart, 
                     fdb_slice_t* mend, uint64_t limit, int reverse, fdb_iterator_t** piterator);


void encode_ssize_key(const char* key, size_t keylen, fdb_slice_t** pslice){
    fdb_slice_t* slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_SSIZE);
    *pslice = slice; 
}


int decode_ssize_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice){ 
    int ret = 0;
    fdb_slice_t *slice_key = NULL;
    fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen);

    uint8_t type = 0;
    if(fdb_bytes_read_uint8(bytes, &type)==-1){
        ret = -1;
        goto end;
    }
    if(type != FDB_DATA_TYPE_SSIZE){
        ret = -1;
        goto end;
    }
    if(fdb_bytes_read_slice_len_left(bytes, &slice_key)==-1){
        ret = -1;
        goto end; 
    }
    *pslice = slice_key;
    fdb_incr_ref_count(slice_key);
    ret = 0;


end:
    fdb_bytes_destroy(bytes);
    fdb_slice_destroy(slice_key);
    return ret;
}

void encode_set_key(const char* key, size_t keylen, const char* member, size_t memberlen, fdb_slice_t** pslice){ 
    fdb_slice_t *slice = fdb_slice_create(key, keylen);
    fdb_slice_uint8_push_front(slice, (uint8_t)keylen);
    fdb_slice_uint8_push_front(slice, FDB_DATA_TYPE_SET);
    fdb_slice_uint8_push_back(slice, '=');
    fdb_slice_string_push_back(slice, member, memberlen);
    *pslice = slice;
}

int decode_set_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t **pkey, fdb_slice_t **pmember){
    int ret = 0;
    fdb_slice_t *slice_key = NULL, *slice_member = NULL;
    fdb_bytes_t *bytes = fdb_bytes_create(fdbkey, fdbkeylen);


    uint8_t  type = 0;
    if(fdb_bytes_read_uint8(bytes, &type)==-1){
        ret = -1;
        goto end;
    }
    if(type != FDB_DATA_TYPE_SET){
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
    if(fdb_bytes_read_slice_len_left(bytes, &slice_member)==-1){
        ret = -1;
        goto end;
    }
    if(pkey!=NULL){
        *pkey = slice_key; 
        fdb_incr_ref_count(slice_key);
    }
    if(pmember!=NULL){
        *pmember = slice_member;
        fdb_incr_ref_count(slice_member);
    }
    ret = 0;

end:
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_member);
    fdb_bytes_destroy(bytes);
    return ret;
}


static int sset_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member){
    if(fdb_slice_length(key)==0 || fdb_slice_length(member)==0){
        fprintf(stderr, "%s empty name or key!\n", __func__); 
        return -1;
    }
    if(fdb_slice_length(key) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s name too long!\n", __func__);
        return -1;
    }
    if(fdb_slice_length(key) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s name too long!\n", __func__);
        return -1;
    }
    int ret = sget_one(context, slot, key, member); 
    if(ret < 0){
        return -1;
    }else{ 
        fdb_slice_t *slice_key = NULL;
        encode_set_key(fdb_slice_data(key),
                       fdb_slice_length(key),
                       fdb_slice_data(member),
                       fdb_slice_length(member),
                       &slice_key);
        fdb_slot_writebatch_put(slot, 
                                fdb_slice_data(slice_key),
                                fdb_slice_length(slice_key),
                                NULL,
                                0);
        fdb_slice_destroy(slice_key);
        return ret==0?1:0;
    }
    return 0;
}

static int sget_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member){  
    fdb_slice_t *slice_key = NULL;
    encode_set_key(fdb_slice_data(key), fdb_slice_length(key), fdb_slice_data(member), fdb_slice_length(member), &slice_key);

    size_t vallen = 0;
    char *errptr = NULL;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    char *val = rocksdb_get_cf(context->db_, readoptions, slot->handle_, fdb_slice_data(slice_key), fdb_slice_length(slice_key), &vallen, &errptr); 
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

static int srem_one(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member){ 
    if(fdb_slice_length(key) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s key too long!\n", __func__);
        return -1;
    }
    if(fdb_slice_length(member) > FDB_DATA_TYPE_KEY_LEN_MAX){
        fprintf(stderr, "%s field too long!\n", __func__);
        return -1;
    }

    if(sget_one(context, slot, key, member) <=0){
        return 0;
    }
    fdb_slice_t *slice_key = NULL;
    encode_set_key(fdb_slice_data(key), fdb_slice_length(key), fdb_slice_data(member), fdb_slice_length(member), &slice_key);

    fdb_slot_writebatch_delete(slot,
                               fdb_slice_data(slice_key),
                               fdb_slice_length(slice_key));
    fdb_slice_destroy(slice_key);
    return 1;
}

static int sget_len(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, uint64_t* length){ 
    char *val = NULL, *errptr = NULL;
    size_t vallen = 0;

    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    fdb_slice_t* slice_key = NULL;
    encode_ssize_key(fdb_slice_data(key), fdb_slice_length(key), &slice_key);
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


static int set_incr_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t by){ 
    uint64_t length = 0;
    int ret = sget_len(context, slot, key, &length);
    if(ret!=0 && ret != 1){
        return -1;
    }
    int64_t size = (int64_t)length;
    size += by;

    fdb_slice_t* slice_key = NULL;
    encode_ssize_key(fdb_slice_data(key),
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
    fdb_slice_destroy(slice_key);
    return 0;
}

static int sget_scan(fdb_context_t* context, fdb_slot_t *slot, fdb_slice_t* key, fdb_slice_t* mstart, 
                     fdb_slice_t* mend, uint64_t limit, int reverse, fdb_iterator_t** piterator){


  fdb_slice_t *slice_start, *slice_end = NULL;
  if(!reverse){

    encode_set_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(mstart),
                    fdb_slice_length(mstart),
                    &slice_start);
    encode_set_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(mend),
                    fdb_slice_length(mend),
                    &slice_end);
    if(fdb_slice_length(mend) == 0){
      fdb_slice_string_push_back(slice_end, "\xff", strlen("\xff"));
    }

    *piterator = fdb_iterator_create(context, slot, slice_start, slice_end, limit, FORWARD);
  }else{
    encode_set_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(mstart),
                    fdb_slice_length(mstart),
                    &slice_start);
    if(fdb_slice_length(mstart) == 0){
      fdb_slice_string_push_back(slice_start, "\xff", strlen("\xff"));
    }
    encode_set_key(fdb_slice_data(key),
                    fdb_slice_length(key),
                    fdb_slice_data(mend),
                    fdb_slice_length(mend),
                    &slice_end);
    *piterator = fdb_iterator_create(context, slot, slice_start, slice_end, limit, BACKWARD);
  }

  fdb_slice_destroy(slice_start);
  fdb_slice_destroy(slice_end);
  return 0;
}


int set_members(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t** rets){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_SET);
    if(retval == FDB_OK){
        fdb_iterator_t* iterator = NULL;
        if(sget_scan(context, slot, key, NULL, NULL, 2000000, 0, &iterator)!=0){
            return FDB_OK_RANGE_HAVE_NONE;
        }
        fdb_array_t *array = fdb_array_create(128);
        do{
            size_t rklen = 0;
            const char* rkey = fdb_iterator_key_raw(iterator, &rklen);
            fdb_slice_t *member = NULL;
            if(decode_set_key(rkey, rklen, NULL, &member)==0){
                fdb_val_node_t* mnode = fdb_val_node_create();
                mnode->retval_ = FDB_OK;
                mnode->val_.vval_ = member;
                fdb_array_push_back(array, mnode);
            } 
        }while(!fdb_iterator_next(iterator));
        fdb_iterator_destroy(iterator);
        *rets = array;
        retval = FDB_OK; 
    }
    return retval;
}

int set_size(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, int64_t* size){ 
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_SET);
    if(retval == FDB_OK){
        uint64_t len = 0;
        int ret =  sget_len(context, slot, key, &len);
        if(ret == 1){
            *size = (int64_t)len;
            retval = FDB_OK;
        }else if(ret == 0){
            *size = 0;
            retval = FDB_OK;
        }else{
            retval = FDB_ERR;
        }
    }
    return retval;
}

int set_member_exists(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_slice_t* member){ 
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_SET);
    if(retval == FDB_OK){
        int ret = sget_one(context, slot, key, member);
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


int set_rem(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t* members, int64_t* count){
    int retval = keys_exs(context, slot, key, FDB_DATA_TYPE_SET);
    if(retval != FDB_OK){
        return retval;
    }

    int64_t _count = 0;
    for(size_t i=0; i<members->length_; ++i){ 
        fdb_slice_t *member = (fdb_slice_t*)(fdb_array_at(members, i)->val_.vval_);
        int ret = srem_one(context, slot, key, member);
        if(ret >= 0){
            if(ret > 0){
                if(set_incr_size(context, slot, key, -1)== 0){
                    _count++;
                }
            }

            char *errptr = NULL;
            fdb_slot_writebatch_commit(context, slot, &errptr);
            if(errptr != NULL){
                fprintf(stderr, "%s fdb_slot_writebatch_commit fail %s.\n", errptr, __func__);
                rocksdb_free(errptr);
            }
        }
    }

    *count = _count;
    return FDB_OK;
}

int set_add(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* key, fdb_array_t* members, int64_t* count){
    int retval = keys_enc(context, slot, key, FDB_DATA_TYPE_SET);
    if(retval != FDB_OK){
        return retval;
    }

    int64_t _count = 0;
    for(size_t i=0; i<members->length_; ++i){ 
        fdb_slice_t *member = (fdb_slice_t*)(fdb_array_at(members, i)->val_.vval_);
        int ret = sset_one(context, slot, key, member);
        if(ret >=0){
            if(ret > 0){
                if(set_incr_size(context, slot, key, 1) == 0){
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
