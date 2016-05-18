#include "fdb_session.h"
#include "fdb_malloc.h"
#include "fdb_context.h"
#include "fdb_slice.h"
#include "fdb_define.h"
#include "fdb_object.h"

#include "t_string.h"
#include "t_keys.h"

#include <rocksdb/c.h>
#include <string.h>
#include <assert.h>


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

int fdb_set(fdb_context_t* context,
            uint64_t id,
            fdb_item_t* key,
            fdb_item_t* val,
            uint64_t exptime, 
            int en){  
    int retval = 0;
    fdb_slot_t* slot = get_slot(context, id);
    fdb_slice_t *slice_key, *slice_val;
    slice_key = fdb_slice_create(key->data_, key->data_len_);
    slice_val = fdb_slice_create(val->data_, val->data_len_);
    if(en == IS_NOT_EXIST){
        retval = string_setnx(context, slot, slice_key, slice_val);
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
             fdb_item_t** vals){
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
                decode_slice_value(&vals_[i], n_val->val_.vval_, n_val->retval_);        
                fdb_slice_destroy(n_val->val_.vval_);
            }else{
                decode_slice_value(&vals_[i], NULL, n_val->retval_);        
            }
            fdb_val_node_t *n_key = fdb_array_at(keys_array, i);
            fdb_slice_destroy(n_key->val_.vval_);
        }
        *vals = vals_;
    }

    fdb_array_destroy(keys_array);
    fdb_array_destroy(rets_array);
    return retval;
}


int fdb_del(fdb_context_t* context, 
            uint64_t id,
            fdb_item_t* key){
    fdb_slot_t* slot = get_slot(context, id);
    fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
    int retval = keys_del(context, slot, slice_key);

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
