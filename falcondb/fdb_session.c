#include "fdb_session.h"
#include "fdb_malloc.h"
#include "fdb_context.h"
#include "fdb_slice.h"
#include "fdb_define.h"

#include "t_string.h"

#include <rocksdb/c.h>
#include <string.h>
#include <assert.h>


void fill_fdb_item(fdb_item_t* item, const char* data, size_t len){
  if(item->data_ != NULL){
    fdb_free(item->data_);
  }
  item->data_ = fdb_malloc(len);
  memcpy(item->data_, data, len);
}

void free_fdb_item(fdb_item_t* item){
  if(item != NULL){
    fdb_free(item->data_);
  }
  item->data_ = NULL;
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
  memset(array, 0, size*sizeof(fdb_item_t));
  return array;
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

static int decode_slice_value(const fdb_slice_t* val, fdb_item_t* item){
  const char* data = fdb_slice_data(val);
  size_t len = fdb_slice_length(val);
  fill_fdb_item(item, data, len); 
  return 0;
}

int fdb_set(fdb_context_t* context,
            fdb_slot_t* slot,
            fdb_item_t* key,
            fdb_item_t* val,
            uint64_t exptime, 
            int en){  
  int retval = 0;
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


int fdb_get(fdb_context_t* context, 
            fdb_slot_t* slot,
            fdb_item_t* key,
            fdb_item_t** pval){  
  int retval = 0;
  fdb_slice_t *slice_key, *slice_val = NULL;
  slice_key = fdb_slice_create(key->data_, key->data_len_);
  retval = string_get(context, slot, slice_key, &slice_val);
  if(retval != FDB_OK){
    goto end;
  }
  *pval = create_fdb_item_array(1);
  if(decode_slice_value(slice_val, *pval)){
    retval = FDB_ERR;
    free_fdb_item(*pval);
    *pval = NULL;
    goto end;
  }
  retval = FDB_OK; 

end:
  fdb_slice_destroy(slice_key);
  fdb_slice_destroy(slice_val);
  return retval;
}


int fdb_del(fdb_context_t* context, 
            fdb_slot_t* slot,
            fdb_item_t* key){
  int retval = 0;
  fdb_slice_t *slice_key = fdb_slice_create(key->data_, key->data_len_);
  retval = string_del(context, slot, slice_key);

  fdb_slice_destroy(slice_key);
  return retval;
}
