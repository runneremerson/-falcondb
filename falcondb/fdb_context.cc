#include "fdb_types.h"
#include "fdb_context.h"
#include "fdb_malloc.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#ifdef __cplusplus
extern "C" { 
#endif

void fdb_drop_db(const char*name){
    char *rocksdb_error = NULL;
    rocksdb_options_t *options = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(options, 1);
    rocksdb_destroy_db(options, name, &rocksdb_error);
    rocksdb_options_destroy(options);
    if(rocksdb_error!=NULL){
        fprintf(stderr, "rocksdb_destroy_db error %s\n", rocksdb_error);
        rocksdb_free(rocksdb_error);
    }
}


fdb_context_t* fdb_context_create(const char* name, size_t write_buffer_size, size_t cache_size, size_t num_slots){
    fdb_context_t* context = (fdb_context_t*)(fdb_malloc(sizeof(fdb_context_t)));
    ++num_slots;
    context->num_slots_ = num_slots;
    context->slots_ = NULL;
    context->block_cache_ = rocksdb_cache_create_lru(cache_size*1024*1024);
    rocksdb_filterpolicy_t *policy = rocksdb_filterpolicy_create_bloom(10);
    context->table_options_ = rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_block_cache(context->table_options_, context->block_cache_);
    rocksdb_block_based_options_set_filter_policy(context->table_options_, policy);
    context->options_ = rocksdb_options_create();
    rocksdb_options_set_max_open_files(context->options_, 10000);
    rocksdb_options_set_create_if_missing(context->options_, 1);
    rocksdb_options_set_create_missing_column_families(context->options_, 1);
    rocksdb_options_set_write_buffer_size(context->options_, write_buffer_size*1024*1024);
    rocksdb_options_set_block_based_table_factory(context->options_, context->table_options_);
    rocksdb_options_set_compression(context->options_, rocksdb_snappy_compression); 

    int num_column_families = (int)num_slots;
    char** slot_names = (char**)fdb_malloc(num_slots * sizeof(char*));
    for(size_t i=0; i<num_slots; ++i){
        char* buff = (char*)fdb_malloc(64);
        memset(buff, 0, 64);
        if(i==0){
            memcpy(buff, "default", strlen("default")); 
        }else{
            sprintf(buff, "slot-%lu", i);
        }
        slot_names[i] = buff; 
    }

    const char** column_family_names = (const char**)slot_names;
    const rocksdb_options_t **column_family_options = (const rocksdb_options_t**)fdb_malloc(num_slots * sizeof(rocksdb_options_t*));
    for(size_t i=0; i<num_slots; ++i){
        column_family_options[i] = context->options_;
    }
    rocksdb_column_family_handle_t **column_family_handles = (rocksdb_column_family_handle_t**)malloc(num_slots * sizeof(rocksdb_column_family_handle_t*));
    
    char* rocksdb_error = NULL;
    fdb_slot_t** slots = NULL;
    context->db_ = rocksdb_open_column_families(context->options_, 
                                                name, 
                                                num_column_families, 
                                                column_family_names, 
                                                column_family_options, 
                                                column_family_handles, 
                                                &rocksdb_error); 
    for(size_t i=0; i<num_slots; ++i){
        fdb_free(slot_names[i]);
    }
    fdb_free(slot_names);
    if(rocksdb_error!=NULL){
        fprintf(stderr, "rocksdb_open_column_families error %s\n", rocksdb_error);
        goto err;
    }

    slots = (fdb_slot_t**)fdb_malloc(num_slots * sizeof(fdb_slot_t*)); 
    for(size_t i=0; i<num_slots; ++i){
        slots[i] = (fdb_slot_t*)fdb_malloc(sizeof(fdb_slot_t));
        slots[i]->id_ = (uint64_t)i;
        slots[i]->handle_ = column_family_handles[i];
        slots[i]->keys_cache_ = rocksdb_cache_create_lru(1024*1024*20);
        slots[i]->batch_ = rocksdb_writebatch_create();
        slots[i]->mutex_ = rocksdb_mutex_create();
    }
    context->slots_ = slots;
    fdb_free(column_family_options);
    fdb_free(column_family_handles);
    return context;

err:
    if(context->db_!=NULL){
        rocksdb_close(context->db_);
    }
    if(context->block_cache_!=NULL){
        rocksdb_cache_destroy(context->block_cache_);
    }
    if(context->options_!=NULL){
        rocksdb_options_destroy(context->options_);
    }
    if(context->table_options_!=NULL){
        rocksdb_block_based_options_destroy(context->table_options_);
    }
    fdb_free(context);
    return NULL;
}

void fdb_context_destroy(fdb_context_t* context){
    if(context!=NULL){
        if(context->slots_!=NULL){
            size_t num_slots = context->num_slots_;
            fdb_slot_t **slots = (fdb_slot_t**)context->slots_;
            for(size_t i=0; i<num_slots; ++i){
                if(slots[i]->handle_ !=NULL){
                    rocksdb_column_family_handle_destroy(slots[i]->handle_);
                }
                rocksdb_cache_destroy(slots[i]->keys_cache_);
                rocksdb_writebatch_destroy(slots[i]->batch_);
                rocksdb_mutex_destroy(slots[i]->mutex_); 
                fdb_free((void*)slots[i]);
            }
            fdb_free((void*)slots);
        }

        rocksdb_close(context->db_);
        rocksdb_cache_destroy(context->block_cache_);
        rocksdb_options_destroy(context->options_);
        rocksdb_block_based_options_destroy(context->table_options_);
    }
    fdb_free(context); 
}

void fdb_context_drop_slot(fdb_context_t* context, fdb_slot_t* slot){
    char *rocksdb_error = NULL;
    rocksdb_drop_column_family(context->db_, slot->handle_, &rocksdb_error);
    if(rocksdb_error!=NULL){ 
        fprintf(stderr, "%s rocksdb_drop_column_family fail %s.\n", __func__, rocksdb_error);
        rocksdb_free(rocksdb_error);
    }else{
        rocksdb_column_family_handle_destroy(slot->handle_);
        slot->handle_ = NULL;
        rocksdb_cache_destroy(slot->keys_cache_);
    }
}

void fdb_context_create_slot(fdb_context_t* context, fdb_slot_t* slot){
    char *rocksdb_error = NULL;
    char buff[64] = {0};
    sprintf(buff, "slot-%lu", (size_t)slot->id_);
    slot->handle_ = rocksdb_create_column_family(context->db_, context->options_, buff, &rocksdb_error);
    if(rocksdb_error!=NULL){
        fprintf(stderr, "%s rocksdb_create_column_family fail %s.\n", __func__, rocksdb_error);
        rocksdb_free(rocksdb_error); 
    }else{ 
        slot->keys_cache_ = rocksdb_cache_create_lru(1024*1024*20);
    }
}

void fdb_slot_writebatch_put(fdb_slot_t* slot, const char* key, size_t klen, const char* val, size_t vlen){
    rocksdb_mutex_lock(slot->mutex_);
    rocksdb_writebatch_put_cf(slot->batch_, slot->handle_, key, klen, val, vlen); 
    rocksdb_mutex_unlock(slot->mutex_);
}

void fdb_slot_writebatch_delete(fdb_slot_t* slot, const char* key, size_t klen){
    rocksdb_mutex_lock(slot->mutex_);
    rocksdb_writebatch_delete_cf(slot->batch_, slot->handle_, key, klen);
    rocksdb_mutex_unlock(slot->mutex_); 
}

void fdb_slot_writebatch_commit(fdb_context_t* context, fdb_slot_t* slot, char** errptr){
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_mutex_lock(slot->mutex_); 
    rocksdb_write(context->db_, writeoptions, slot->batch_, errptr);
    rocksdb_writebatch_clear(slot->batch_);
    rocksdb_mutex_unlock(slot->mutex_);
    rocksdb_writeoptions_destroy(writeoptions);
}

#ifdef __cplusplus
}
#endif
