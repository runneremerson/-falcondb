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

int* create_int_array(size_t size){
    int *array_ = (int*)fdb_malloc(size*sizeof(int));
    return array_;
}

void free_int_array(void* array){
    fdb_free(array);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
        int *rets_ = create_int_array(len); 
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_slice_t *slice_fld = fdb_slice_create(fld->data_, fld->data_len_);

    int retval = hash_exists(context, slot, slice_key, slice_fld);

    if(retval == FDB_OK){
        *count = 1;
    }else if(retval == FDB_OK_NOT_EXIST){
        *count = 0;
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
    fdb_slot_t* slot = get_slot(context, id);
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
    fdb_slot_t* slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    fdb_array_t *rets_array = NULL;
    
    int retval = hash_getall(context, slot, slice_key, &rets_array);
    if(retval == FDB_OK){
        *length = rets_array->length_;
        fdb_item_t *fld_vals_ = create_fdb_item_array(*length);
        for(size_t i=0; i<(*length);){
            fdb_val_node_t *n_fld = fdb_array_at(rets_array, i);
            if(n_fld->retval_==FDB_OK){
                decode_slice_value(&fld_vals_[i], (fdb_slice_t*)(n_fld->val_.vval_), n_fld->retval_);        
                fdb_slice_destroy(n_fld->val_.vval_);
            }else{
                decode_slice_value(&fld_vals_[i], (fdb_slice_t*)NULL, n_fld->retval_);        
            }
            i++;
            fdb_val_node_t *n_val = fdb_array_at(rets_array, i++);
            if(n_val->retval_==FDB_OK){
                decode_slice_value(&fld_vals_[i], (fdb_slice_t*)(n_val->val_.vval_), n_val->retval_);        
                fdb_slice_destroy(n_val->val_.vval_);
            }else{
                decode_slice_value(&fld_vals_[i], (fdb_slice_t*)NULL, n_val->retval_);         
            }
            i++; 
        } 
        *pfvs = fld_vals_;
    }
    fdb_slice_destroy(slice_key);
    fdb_array_destroy(rets_array);
    return retval;
}



#ifdef __cplusplus
}
#endif
