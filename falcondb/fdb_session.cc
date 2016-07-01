#include "fdb_types.h"
#include "fdb_session.h"
#include "fdb_malloc.h"
#include "fdb_context.h"
#include "fdb_slice.h"
#include "fdb_define.h"
#include "fdb_object.h"

#include "t_keys.h"
#include "t_string.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_set.h"

#include <string.h>
#include <assert.h>
#include <stdio.h>


#ifdef __cplusplus
extern "C" { 
#endif


static void fill_fdb_item(fdb_item_t* item, fdb_slice_t* hold, int retval){
    if(item->hold_ != NULL){
        fdb_slice_destroy(item->hold_);
    }
    if(hold != NULL){
        fdb_incr_ref_count(hold);
        item->data_ = fdb_slice_data(hold);
        item->data_len_ = fdb_slice_length(hold);
    }else{
        item->data_ = NULL;
        item->data_len_ = 0;
    }
    item->hold_ = hold;
    item->retval_ = retval;
}

void free_fdb_item(fdb_item_t* item){
    if(item != NULL){
        fdb_slice_destroy(item->hold_);
    }
}

void destroy_fdb_item_array( fdb_item_t* array, size_t size){
    for( size_t i=0; i<size; ++i){
        free_fdb_item(&array[i]);
    }
    fdb_free(array);
}

fdb_item_t* create_fdb_item_array( size_t size){
    if( size == 0){
        return NULL;
    }
    fdb_item_t *array = (fdb_item_t*)fdb_malloc(size * sizeof(fdb_item_t));
    for(size_t i=0; i<size; ++i){
        array[i].hold_ = NULL;
        array[i].data_ = NULL;
        array[i].data_len_ = 0;
        array[i].retval_ = 0;
    }
    return array;
}

static void* create_primitive_array(size_t num, size_t size){
    return fdb_malloc(num*size);
}

static void free_primitive_array(void* array){
    fdb_free(array);
}

double* create_double_array(size_t num){
   return (double*)create_primitive_array(num, sizeof(double)); 
}

int* create_int_array(size_t num){
    return (int*)create_primitive_array(num, sizeof(int));    
}

void free_int_array(int* array){
    free_primitive_array(array);
}
void free_double_array(double* array){ 
    free_primitive_array(array);
}







enum ExistOrNot
{
    IS_NOT_EXIST = 0, //nx
    IS_EXIST = 1,
    IS_NOT_EXIST_AND_EXPIRE = 2,
    IS_EXIST_AND_EXPIRE = 3
};

char g_programname[1024] = {0};

int set_fdb_signal_handler(const char* name){
    return 0;
}

static void decode_slice_value(fdb_item_t* item, fdb_slice_t* val, int retval){
    fill_fdb_item(item, val, retval); 
}

static fdb_slot_t* get_slot(fdb_context_t* context, uint64_t id){
    fdb_slot_t** slot = (fdb_slot_t**)(context->slots_);
    return slot[id];
}

void fdb_drop_slot(fdb_context_t* context, uint64_t id){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_context_drop_slot(context, slot);
    fdb_context_create_slot(context, slot);
}


int fdb_set(fdb_context_t* context,
            uint64_t id,
            fdb_item_t* key,
            fdb_item_t* val,
            int64_t duration, 
            int en, int* ef){  
    int retval = 0;
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key, *slice_val;
    slice_key = fdb_slice_create(key->data_, key->data_len_);
    slice_val = fdb_slice_create(val->data_, val->data_len_);
    if(en == IS_NOT_EXIST){
        retval = string_setnx(context, slot, slice_key, slice_val);
        if(retval == FDB_OK_BUT_ALREADY_EXIST){
            *ef = 0;
        }else if(retval == FDB_OK){
            *ef = 1;
        }
        
    } else if(en == IS_EXIST){
        retval = string_set(context, slot, slice_key, slice_val);
    } else if(en == IS_EXIST_AND_EXPIRE){
    } else if(en == IS_NOT_EXIST_AND_EXPIRE){
    }

    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_val);

    return retval;
}

int fdb_mset(fdb_context_t* context,
             uint64_t id,
             size_t length,
             fdb_item_t* kvs,
             int** rets,
             int en){
    int retval = 0;
    fdb_slot_t *slot = get_slot(context, id);
    fdb_array_t *kvs_array = fdb_array_create(16);
    for(size_t i=0; i<length; ){
        fdb_val_node_t *n_key = fdb_val_node_create();
        n_key->val_.vval_ = fdb_slice_create(kvs[i].data_, kvs[i].data_len_);
        fdb_array_push_back(kvs_array, n_key);
        ++i;

        fdb_val_node_t *n_val = fdb_val_node_create();
        n_val->val_.vval_ = fdb_slice_create(kvs[i].data_, kvs[i].data_len_);
        fdb_array_push_back(kvs_array, n_val);
        ++i;
    }

    fdb_array_t *rets_array = NULL;
    if(en == IS_NOT_EXIST){
        retval = string_msetnx(context, slot, kvs_array, &rets_array);
    }else{
        retval = string_mset(context, slot, kvs_array, &rets_array);
    }

    if(retval == FDB_OK){
        int len = length/2;
        int *rets_ = (int*)create_int_array(len); 
        for(int i=0; i<len; ++i){
            fdb_val_node_t *n_ret = fdb_array_at(rets_array, i);
            rets_[i] = n_ret->retval_;
            //printf("fdb--len=%d, rets[%d]=%d\n", len, i, n_ret->retval_);
        }
        *rets = rets_;
    }

    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_ = fdb_array_at(kvs_array, i);
        fdb_slice_destroy(n_->val_.vval_);
    }

    fdb_array_destroy(rets_array);
    fdb_array_destroy(kvs_array);
    return retval;
}


int fdb_get(fdb_context_t* context, 
            uint64_t id,
            fdb_item_t* key,
            fdb_item_t** pval){  
    int retval = 0;
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key, *slice_val = NULL;
    slice_key = fdb_slice_create(key->data_, key->data_len_);
    retval = string_get(context, slot, slice_key, &slice_val);
    if(retval != FDB_OK){
        goto end;
    }
    *pval = create_fdb_item_array(1);
    retval = FDB_OK; 
    decode_slice_value(*pval, slice_val, retval);

end:
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_val);
    return retval;
}

int fdb_mget(fdb_context_t* context,
             uint64_t id,
             size_t length,
             fdb_item_t* keys,
             fdb_item_t** pvals){
    int retval = 0;
    fdb_slot_t *slot = get_slot(context, id);
    fdb_array_t *keys_array = fdb_array_create(16);
    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_key = fdb_val_node_create();
        n_key->val_.vval_ = fdb_slice_create(keys[i].data_, keys[i].data_len_);
        fdb_array_push_back(keys_array, n_key);
    }
    fdb_array_t *rets_array = NULL;
    retval = string_mget(context, slot, keys_array, &rets_array);
    if(retval == FDB_OK){
        fdb_item_t *vals_ = create_fdb_item_array(length);
        for(size_t i=0; i<length; ++i){
            fdb_val_node_t *n_val = fdb_array_at(rets_array, i);
            if(n_val->retval_==FDB_OK){
                decode_slice_value(&vals_[i], (fdb_slice_t*)(n_val->val_.vval_), n_val->retval_);        
                fdb_slice_destroy(n_val->val_.vval_);
            }else{
                decode_slice_value(&vals_[i], (fdb_slice_t*)NULL, n_val->retval_);        
            }
        }
        *pvals = vals_;
    }

    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_key = fdb_array_at(keys_array, i);
        fdb_slice_destroy(n_key->val_.vval_);
    }

    fdb_array_destroy(keys_array);
    fdb_array_destroy(rets_array);
    return retval;
}


int fdb_del(fdb_context_t* context, 
            uint64_t id,
            fdb_item_t* key,
            int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    int64_t _count = 0;
    int retval = keys_del(context, slot, slice_key, &_count);
    if(retval == FDB_OK){
        *count = _count; 
    }
    fdb_slice_destroy(slice_key);
    return retval;
}

int fdb_incrby(fdb_context_t* context,
               uint64_t id,
               fdb_item_t* key,
               int64_t init,
               int64_t by,
               int64_t *result){
    int retval = 0;
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    retval = string_incr(context, slot, slice_key, init, by, result);

    fdb_slice_destroy(slice_key);
    return retval; 
}


int fdb_hset(fdb_context_t* context,
        uint64_t id,
        fdb_item_t* key,
        fdb_item_t* fld,
        fdb_item_t* val,
        int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_fld = fdb_slice_create(fld->data_, fld->data_len_);
    fdb_slice_t *slice_val = fdb_slice_create(val->data_, val->data_len_);

    int64_t _count = 0;
    int retval = hash_set(context, slot, slice_key, slice_fld, slice_val, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }

    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_val);
    fdb_slice_destroy(slice_fld);
    return retval;
}

int fdb_hsetnx(fdb_context_t* context,
               uint64_t id,
               fdb_item_t* key,
               fdb_item_t* fld,
               fdb_item_t* val,
               int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_fld = fdb_slice_create(fld->data_, fld->data_len_);
    fdb_slice_t *slice_val = fdb_slice_create(val->data_, val->data_len_);

    int64_t _count = 0;
    int retval = hash_setnx(context, slot, slice_key, slice_fld, slice_val, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }

    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_val);
    fdb_slice_destroy(slice_fld);
    return retval;
}


int fdb_hget(fdb_context_t* context,
        uint64_t id,
        fdb_item_t* key,
        fdb_item_t* fld,
        fdb_item_t** pval){ 
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_fld = fdb_slice_create(fld->data_, fld->data_len_);
    fdb_slice_t *slice_val = NULL;

    int retval = hash_get(context, slot, slice_key, slice_fld, &slice_val);
    if(retval != FDB_OK){
        goto end;
    }
    *pval = create_fdb_item_array(1);
    retval = FDB_OK; 
    decode_slice_value(*pval, slice_val, retval);

end:
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_fld);
    fdb_slice_destroy(slice_val);
    return retval;
}

int fdb_hmget(fdb_context_t* context,
              uint64_t id,
              fdb_item_t* key,
              size_t length,
              fdb_item_t* flds,
              fdb_item_t** pvals){              
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *flds_array = fdb_array_create(8), *rets_array = NULL;
    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_fld = fdb_val_node_create();
        n_fld->val_.vval_ = fdb_slice_create(flds[i].data_, flds[i].data_len_);
        fdb_array_push_back(flds_array, n_fld);
    }
    int retval = hash_mget(context, slot, slice_key, flds_array, &rets_array);
    if(retval == FDB_OK){
        fdb_item_t *vals_ = create_fdb_item_array(length);
        for(size_t i=0; i<length; ++i){
            fdb_val_node_t *n_val = fdb_array_at(rets_array, i);
            if(n_val->retval_==FDB_OK){
                decode_slice_value(&vals_[i], (fdb_slice_t*)(n_val->val_.vval_), n_val->retval_);        
                fdb_slice_destroy(n_val->val_.vval_);
            }else{
                decode_slice_value(&vals_[i], (fdb_slice_t*)NULL, n_val->retval_);        
            }
        }
        *pvals = vals_;
    }

    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_fld = fdb_array_at(flds_array, i);
        fdb_slice_destroy(n_fld->val_.vval_);
    } 

    fdb_slice_destroy(slice_key);
    fdb_array_destroy(flds_array);
    fdb_array_destroy(rets_array);
    return retval;
}

int fdb_hdel(fdb_context_t* context,
             uint64_t id,
             fdb_item_t* key,
             size_t length,
             fdb_item_t* flds,
             int64_t *count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *flds_array = fdb_array_create(8);
    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_field = fdb_val_node_create();
        n_field->val_.vval_ = fdb_slice_create(flds[i].data_, flds[i].data_len_);
        fdb_array_push_back(flds_array, n_field);
    }
    int64_t _count = 0;
    int retval = hash_del(context, slot, slice_key, flds_array, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }
    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_field = fdb_array_at(flds_array, i);
        fdb_slice_destroy(n_field->val_.vval_);
    }
    fdb_array_destroy(flds_array);

    return retval;
}

int fdb_hlen(fdb_context_t* context,
             uint64_t id,
             fdb_item_t* key,
             int64_t *length){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);

    int64_t _length = 0;
    int64_t retval = hash_length(context, slot, slice_key, &_length);
    if(retval == FDB_OK){
        *length = _length;
    }
    return retval;
}


int fdb_hincrby(fdb_context_t* context,
                uint64_t id,
                fdb_item_t* key,
                fdb_item_t* fld,
                int64_t by,
                int64_t* result){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_fld = fdb_slice_create(fld->data_, fld->data_len_);

    int64_t _result = 0;
    int retval = hash_incr(context, slot, slice_key, slice_fld, 0, by, &_result);
    if(retval == FDB_OK){
        *result = _result;
    }
    
    fdb_slice_destroy(slice_key); 
    fdb_slice_destroy(slice_fld);
    return retval; 
}

int fdb_hexists(fdb_context_t* context,
                uint64_t id,
                fdb_item_t* key,
                fdb_item_t* fld,
                int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_fld = fdb_slice_create(fld->data_, fld->data_len_);

    int64_t _count = 0;
    int retval = hash_exists(context, slot, slice_key, slice_fld, &_count);

    if(retval == FDB_OK){
        *count = _count;
    }
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_fld);
    return retval;
}

int fdb_hmset(fdb_context_t* context,
              uint64_t id,
              fdb_item_t* key,
              size_t length,
              fdb_item_t* fvs,
              int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *fvs_array = fdb_array_create(8);
    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *node = fdb_val_node_create();
        node->val_.vval_ = fdb_slice_create(fvs[i].data_, fvs[i].data_len_);
        fdb_array_push_back(fvs_array, node);
    }
    int64_t _count = 0;
    int retval = hash_mset(context, slot, slice_key, fvs_array, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }
    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *node = fdb_array_at(fvs_array, i);
        fdb_slice_destroy(node->val_.vval_); 
    }
    fdb_array_destroy(fvs_array);
    fdb_slice_destroy(slice_key); 
    return retval; 
}

int fdb_hgetall(fdb_context_t* context,
                uint64_t id,
                fdb_item_t* key,
                fdb_item_t** pfvs,
                int64_t* length){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *rets_array = NULL;
    
    int retval = hash_getall(context, slot, slice_key, &rets_array);
    if(retval == FDB_OK && rets_array->length_>0){
        *length = rets_array->length_;
        fdb_item_t *_fld_vals = create_fdb_item_array(*length);
        for(size_t i=0; i<(*length);){
            fdb_val_node_t *n_fld = fdb_array_at(rets_array, i);
            if(n_fld->retval_==FDB_OK){
                decode_slice_value(&_fld_vals[i], (fdb_slice_t*)(n_fld->val_.vval_), n_fld->retval_);        
                fdb_slice_destroy(n_fld->val_.vval_);
            }else{
                decode_slice_value(&_fld_vals[i], (fdb_slice_t*)NULL, n_fld->retval_);        
            }
            i++;
            fdb_val_node_t *n_val = fdb_array_at(rets_array, i);
            if(n_val->retval_==FDB_OK){
                decode_slice_value(&_fld_vals[i], (fdb_slice_t*)(n_val->val_.vval_), n_val->retval_);        
                fdb_slice_destroy(n_val->val_.vval_);
            }else{
                decode_slice_value(&_fld_vals[i], (fdb_slice_t*)NULL, n_val->retval_);         
            }
            i++; 
        } 
        *pfvs = _fld_vals;
    }
    fdb_slice_destroy(slice_key);
    fdb_array_destroy(rets_array);
    return retval;
}

int fdb_zadd(fdb_context_t* context,
             uint64_t id,
             fdb_item_t* key,
             size_t length,
             double* scores,
             fdb_item_t* members,
             int64_t* count){
                 
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *sms_array = fdb_array_create(8);
    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_score = fdb_val_node_create();
        n_score->val_.dval_ = scores[i];
        fdb_array_push_back(sms_array, n_score);

        fdb_val_node_t *n_member = fdb_val_node_create();
        n_member->val_.vval_ = fdb_slice_create(members[i].data_, members[i].data_len_);
        fdb_array_push_back(sms_array, n_member);
    }
    int64_t _count = 0;
    int retval = zset_add(context, slot, slice_key, sms_array, &_count);
    if(retval == FDB_OK){
        *count = _count; 
    }
    size_t ind = 0;
    for(int i=0; i<length; ++i){ 
        //skip score
        ++ind;
        fdb_val_node_t *n_member = fdb_array_at(sms_array, ind);
        fdb_slice_destroy(n_member->val_.vval_);
        ++ind; 
    }
    fdb_slice_destroy(slice_key);
    fdb_array_destroy(sms_array);
    return retval;
}

int fdb_zrem(fdb_context_t* context,
             uint64_t id,
             fdb_item_t* key,
             size_t length,
             fdb_item_t* members,
             int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *mbr_array = fdb_array_create(8);

    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_member = fdb_val_node_create();
        n_member->val_.vval_ = fdb_slice_create(members[i].data_, members[i].data_len_);
        fdb_array_push_back(mbr_array, n_member); 
    }
    int64_t _count = 0;
    int retval = zset_rem(context, slot, slice_key, mbr_array, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }
    for(int i=0; i<length; ++i){
        fdb_val_node_t *n_member = fdb_array_at(mbr_array, i);
        fdb_slice_destroy(n_member->val_.vval_); 
    }
    fdb_slice_destroy(slice_key);
    fdb_array_destroy(mbr_array);
    return retval;
}

int fdb_zcard(fdb_context_t* context,
              uint64_t id,
              fdb_item_t* key,
              int64_t* size){ 
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);

    int64_t _size = 0;
    int retval = zset_size(context, slot, slice_key, &_size);
    if(retval == FDB_OK){
        *size = _size;
    }
    fdb_slice_destroy(slice_key);
    return retval;
}

int fdb_zscore(fdb_context_t* context,
               uint64_t id,
               fdb_item_t* key,
               fdb_item_t* mbr,
               double* score){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_mbr = fdb_slice_create(mbr->data_, mbr->data_len_);

    double _score = 0.0;
    int retval = zset_score(context, slot, slice_key, slice_mbr, &_score);
    if(retval == FDB_OK){
        *score = _score;
    }
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_mbr);
    return retval;
}

int fdb_zcount(fdb_context_t* context,
               uint64_t id,
               fdb_item_t* key,
               double start,
               double end,
               uint8_t type,
               int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    
    int64_t _count = 0;
    int retval = zset_count(context, slot, slice_key, start, end, type, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }
    fdb_slice_destroy(slice_key); 
    return retval;
}


int fdb_zrem_range_by_rank(fdb_context_t* context,
                           uint64_t id,
                           fdb_item_t* key,
                           int start,
                           int end,
                           int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    
    int64_t _count = 0;
    int retval = zset_rem_range_by_rank(context, slot, slice_key, start, end, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }
    fdb_slice_destroy(slice_key);
    return retval;
}

int fdb_zrem_range_by_score(fdb_context_t* context,
                            uint64_t id,
                            fdb_item_t* key,
                            double start,
                            double end,
                            uint8_t type,
                            int64_t *count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);

    int64_t _count = 0;
    int retval = zset_rem_range_by_score(context, slot, slice_key, start, end, type, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }
    fdb_slice_destroy(slice_key);
    return retval;
}

int fdb_zrange(fdb_context_t* context,
               uint64_t id,
               fdb_item_t* key,
               int start,
               int end,
               int reverse,
               double** pscores,
               fdb_item_t** pmembers,
               int64_t* length){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *sms_array = NULL;
    
    int retval = zset_range(context, slot, slice_key, start, end, reverse, &sms_array);
    if(retval == FDB_OK && sms_array->length_>0){
        size_t len = sms_array->length_ /2;
        *length = len;
        double *_scores = (double*)create_double_array(len);
        fdb_item_t *_members = create_fdb_item_array(len);
        for(size_t ind=0, i=0; i<len; ++i){ 
            fdb_val_node_t *n_score = fdb_array_at(sms_array, ind++);
            double score = n_score->val_.dval_;
            _scores[i] = score;

            fdb_val_node_t *n_member = fdb_array_at(sms_array, ind++);
            decode_slice_value(&_members[i], (fdb_slice_t*)(n_member->val_.vval_), n_member->retval_);        
            fdb_slice_destroy(n_member->val_.vval_); 
        } 
        *pscores = _scores;
        *pmembers = _members;
    }
    fdb_slice_destroy(slice_key);
    fdb_array_destroy(sms_array);

    return retval;
}

int fdb_zrank(fdb_context_t* context,
              uint64_t id,
              fdb_item_t* key,
              fdb_item_t* mbr,
              int reverse,
              int64_t* rank){ 
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_mbr = fdb_slice_create(mbr->data_, mbr->data_len_);

    int64_t _rank = 0;
    int retval = zset_rank(context, slot, slice_key, slice_mbr, reverse, &_rank);
    if(retval == FDB_OK){
        *rank = _rank;
    }

    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_mbr);
    return retval;
}

int fdb_zincrby(fdb_context_t* context,
                uint64_t id,
                fdb_item_t* key,
                fdb_item_t* mbr,
                double by,
                double* result){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_mbr = fdb_slice_create(mbr->data_, mbr->data_len_);
    
    double _result = 0.0;
    int retval = zset_incr(context, slot, slice_key, slice_mbr, 0.0, by, &_result);
    if(retval == FDB_OK){
        *result = _result;
    }
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_mbr);
    return retval;
}

int fdb_smembers(fdb_context_t* context,
                 uint64_t id,
                 fdb_item_t* key,
                 fdb_item_t** pmembers,
                 int64_t* length){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
 
    fdb_array_t *mbr_array = NULL;
    int retval = set_members(context, slot, slice_key, &mbr_array);
    if(retval == FDB_OK && mbr_array->length_>0){
        *length = mbr_array->length_;
        fdb_item_t *_members = create_fdb_item_array(*length);
        for(size_t i=0; i<(*length); ++i){ 
            fdb_val_node_t *n_member = fdb_array_at(mbr_array, i);
            if(n_member->retval_==FDB_OK){
                decode_slice_value(&_members[i], (fdb_slice_t*)(n_member->val_.vval_), n_member->retval_);        
                fdb_slice_destroy(n_member->val_.vval_);
            }else{
                decode_slice_value(&_members[i], (fdb_slice_t*)NULL, n_member->retval_);        
            }
        } 
        *pmembers = _members;
    }

    fdb_slice_destroy(slice_key);
    fdb_array_destroy(mbr_array);
    return retval;
}

int fdb_sismember(fdb_context_t* context,
                  uint64_t id,
                  fdb_item_t* key,
                  fdb_item_t* mbr,
                  int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_mbr = fdb_slice_create(mbr->data_, mbr->data_len_);
    
    int64_t _count = 0;
    int retval = set_member_exists(context, slot, slice_key, slice_mbr, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }
    fdb_slice_destroy(slice_key);
    fdb_slice_destroy(slice_mbr);
    return retval;
}

int fdb_scard(fdb_context_t* context,
              uint64_t id,
              fdb_item_t* key,
              int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);

    int64_t _count = 0;
    int retval = set_size(context, slot, slice_key, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }
    fdb_slice_destroy(slice_key);
    return retval;
}

int fdb_srem(fdb_context_t* context,
             uint64_t id,
             fdb_item_t* key,
             size_t length,
             fdb_item_t* members,
             int64_t* count){
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *mbr_array = fdb_array_create(8);

    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_member = fdb_val_node_create();
        n_member->val_.vval_ = fdb_slice_create(members[i].data_, members[i].data_len_);
        fdb_array_push_back(mbr_array, n_member); 
    }
    int64_t _count = 0;
    int retval = set_rem(context, slot, slice_key, mbr_array, &_count);
    if(retval == FDB_OK){
        *count = _count;
    }
    for(int i=0; i<length; ++i){
        fdb_val_node_t *n_member = fdb_array_at(mbr_array, i);
        fdb_slice_destroy(n_member->val_.vval_); 
    }
    fdb_slice_destroy(slice_key);
    fdb_array_destroy(mbr_array);
    return retval;
}

int fdb_sadd(fdb_context_t* context,
             uint64_t id,
             fdb_item_t* key,
             size_t length,
             fdb_item_t* members,
             int64_t* count){
                 
    fdb_slot_t *slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *mbr_array = fdb_array_create(8);
    for(size_t i=0; i<length; ++i){
        fdb_val_node_t *n_member = fdb_val_node_create();
        n_member->val_.vval_ = fdb_slice_create(members[i].data_, members[i].data_len_);
        fdb_array_push_back(mbr_array, n_member);
    }
    int64_t _count = 0;
    int retval = set_add(context, slot, slice_key, mbr_array, &_count);
    if(retval == FDB_OK){
        *count = _count; 
    }
    for(int i=0; i<length; ++i){ 
        fdb_val_node_t *n_member = fdb_array_at(mbr_array, i);
        fdb_slice_destroy(n_member->val_.vval_);
    }
    fdb_slice_destroy(slice_key);
    fdb_array_destroy(mbr_array);
    return retval;
}



#ifdef __cplusplus
}
#endif
