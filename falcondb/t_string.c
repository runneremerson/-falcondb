#include "t_string.h"
#include "t_keys.h"

#include "fdb_define.h"
#include "fdb_slice.h"
#include "fdb_context.h"
#include "fdb_bytes.h"
#include "util.h"

#include <rocksdb/c.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>


int string_set(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t* value){  
    int retval = 0;
    if(fdb_slice_length(key) == 0){
        fprintf(stderr, "%s empty key!\n", __func__);
        retval = FDB_ERR;
        goto end;
    }
    retval = keys_set_string(context, slot, key, value);

end:
    return retval;
}

int string_setnx(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t* value){  
    int retval = FDB_OK;
    if(fdb_slice_length(key) == 0){
        fprintf(stderr, "%s empty key!\n", __func__);
        retval = FDB_ERR;
        goto end;
    }
    //get
    fdb_slice_t *slice_value = NULL;
    int found = keys_get_string(context, slot,  key, &slice_value);
    if(found == FDB_OK){
        retval = FDB_OK_BUT_ALREADY_EXIST;
        goto end;
    }
    retval = keys_set_string(context, slot, key, value);

end:
    fdb_slice_destroy(slice_value);
    return retval;
}


int string_setxx(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t* value){  
    int retval = FDB_OK;
    if(fdb_slice_length(key) == 0){
        fprintf(stderr, "%s empty key!\n", __func__);
        retval = FDB_ERR;
        goto end;
    }

    //get
    fdb_slice_t *slice_value = NULL;
    int found = keys_get_string(context, slot,  key, &slice_value);
    if(found != FDB_OK){
        retval = found;
        goto end;
    } 
    //set
    retval = keys_set_string(context, slot, key, value);

end:
    fdb_slice_destroy(slice_value);
    return retval;
}


int string_get(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t** pvalue){
    return keys_get_string(context, slot, key, pvalue);
}

static int is_int64_overflow(int64_t val, int64_t by){ 
    if(by<0 && val<0){
        if((INT64_MIN-by)>val){
            return 1;
        }
    }else if(by>0 && val>0){
        if((INT64_MAX-by)<val){
            return 1;
        }
    }
    return 0;
}


int string_incr(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, int64_t init, int64_t by, int64_t* val){   
    int retval = 0;

    fdb_slice_t *slice_value = NULL;
    int ret = keys_get_string(context, slot, key, &slice_value);
    if(ret==FDB_OK){
        if(fdb_slice_length(slice_value) != sizeof(int64_t)){
            retval = FDB_ERR_IS_NOT_INTEGER;
            goto end;
        }
        uint64_t _old = rocksdb_decode_fixed64(fdb_slice_data(slice_value));
        int64_t *old = (int64_t*)&_old; 
        if(is_int64_overflow(*old, by)){
            retval = FDB_ERR_INCDECR_OVERFLOW;
            goto end;
        }
        char buff[sizeof(int64_t)] = {0};
        rocksdb_encode_fixed64(buff, *old + by);
        fdb_slice_destroy(slice_value);
        slice_value = fdb_slice_create(buff, sizeof(int64_t));
        retval = keys_set_string(context, slot, key, slice_value);
    }else if(ret == FDB_OK_NOT_EXIST){
        if(is_int64_overflow(init, by)){
            retval = FDB_ERR_INCDECR_OVERFLOW;
            goto end; 
        }
        char buff[sizeof(int64_t)] = {0};
        rocksdb_encode_fixed64(buff, init + by);
        fdb_slice_destroy(slice_value);
        slice_value = fdb_slice_create(buff, sizeof(int64_t));
        retval = keys_set_string(context, slot, key, slice_value); 
    }else{
        retval = FDB_ERR;
    }

end:
    if(slice_value!=NULL) fdb_slice_destroy(slice_value);
    return retval;
}
