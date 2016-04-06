#include "t_string.h"

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



void encode_kv_key(const char* key, size_t keylen, fdb_slice_t** pslice){
  fdb_slice_t* slice = fdb_slice_create(key, keylen);
  fdb_slice_string_push_front(slice, FDB_DATA_TYPE_STRING, strlen(FDB_DATA_TYPE_STRING));
  *pslice =  slice;
}

int decode_kv_key(const char* fdbkey, size_t fdbkeylen, fdb_slice_t** pslice){
  int retval = 0;
  fdb_slice_t *slice_key = NULL;
  fdb_bytes_t* bytes = fdb_bytes_create(fdbkey, fdbkeylen);

  if(compare_with_length(fdbkey, strlen(FDB_DATA_TYPE_STRING), FDB_DATA_TYPE_STRING, strlen(FDB_DATA_TYPE_STRING))!=0){
    goto err;
  }
  if(fdb_bytes_skip(bytes, strlen(FDB_DATA_TYPE_STRING)) == -1 ){
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


int string_set(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, const fdb_slice_t* value){  
  int retval = 0;
  if(fdb_slice_length(key) == 0){
    fprintf(stderr, "%s empty key!\n", __func__);
    retval = FDB_ERR;
    goto end;
  }
  char *errptr = NULL;
  rocksdb_writeoptions_t* writeoptions = rocksdb_writeoptions_create();
  fdb_slice_t *slice_key = NULL;
  encode_kv_key(fdb_slice_data(key), fdb_slice_length(key), &slice_key);
  rocksdb_put_cf(context->db_, 
                writeoptions, 
                slot->handle_,
                fdb_slice_data(slice_key), 
                fdb_slice_length(slice_key), 
                fdb_slice_data(value), 
                fdb_slice_length(value), 
                &errptr);
  rocksdb_writeoptions_destroy(writeoptions);
  fdb_slice_destroy(slice_key);
  if(errptr != NULL){
    fprintf(stderr, "%s rocksdb_put_cf failed %s.\n", __func__, errptr);
    rocksdb_free(errptr);
    retval = FDB_ERR;
    goto end;
  }
  retval = FDB_OK;

end:
  return retval;
}

int string_setnx(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, const fdb_slice_t* value){  
  int retval = FDB_OK;
  if(fdb_slice_length(key) == 0){
    fprintf(stderr, "%s empty key!\n", __func__);
    retval = FDB_ERR;
    goto end;
  }
  //get
  fdb_slice_t *slice_value = NULL;
  int found = string_get(context, slot,  key, &slice_value);
  if(found == FDB_OK){
    retval = FDB_OK_BUT_ALREADY_EXIST;
    goto end;
  }
  //set
  char *errptr = NULL;
  rocksdb_writeoptions_t* writeoptions = rocksdb_writeoptions_create();
  fdb_slice_t *slice_key = NULL;
  encode_kv_key(fdb_slice_data(key), fdb_slice_length(key), &slice_key);
  rocksdb_put_cf(context->db_, 
                writeoptions, 
                slot->handle_,
                fdb_slice_data(slice_key), 
                fdb_slice_length(slice_key), 
                fdb_slice_data(value), 
                fdb_slice_length(value), 
                &errptr);
  rocksdb_writeoptions_destroy(writeoptions);
  fdb_slice_destroy(slice_key);
  if(errptr != NULL){
    fprintf(stderr, "%s rocksdb_put_cf failed %s.\n", __func__, errptr);
    rocksdb_free(errptr);
    retval = FDB_ERR;
    goto end;
  }
  retval = FDB_OK; 

end:
  fdb_slice_destroy(slice_value);
  return retval;
}


int string_get(fdb_context_t* context, fdb_slot_t* slot, const fdb_slice_t* key, fdb_slice_t** pvalue){
  char *val, *errptr = NULL;
  size_t vallen = 0;
  rocksdb_readoptions_t* readoptions = rocksdb_readoptions_create();
  fdb_slice_t *slice_key = NULL;
  encode_kv_key(fdb_slice_data(key), fdb_slice_length(key), &slice_key);
  val = rocksdb_get_cf(context->db_, readoptions, slot->handle_, fdb_slice_data(slice_key), fdb_slice_length(slice_key), &vallen, &errptr);
  rocksdb_readoptions_destroy(readoptions);
  fdb_slice_destroy(slice_key);
  int retval = FDB_OK;
  if(errptr != NULL){
    fprintf(stderr, "%s rocksdb_get_cf fail %s.\n", __func__, errptr);
    rocksdb_free(errptr);
    retval = FDB_ERR;
    goto end;
  }
  if(val != NULL){
  }

end:
  if(val != NULL){
    rocksdb_free(val);
  }
  return retval;
}
