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
