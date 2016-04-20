#include "fdb_context.h"
#include "fdb_malloc.h"

#include <rocksdb/c.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>


fdb_context_t* fdb_context_create(const char* name, size_t write_buffer_size, size_t cache_size, int num_slots, const char** slot_names, fdb_slot_t** slots){
    fdb_context_t* context = (fdb_context_t*)(fdb_malloc(sizeof(fdb_context_t)));
    context->block_cache_ = rocksdb_cache_create_lru(cache_size*1024*1024);
    context->table_options_ = rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_block_cache(context->table_options_, context->block_cache_);
    rocksdb_block_based_options_set_filter_policy(context->table_options_, context->filter_policy_);
    context->filter_policy_ = rocksdb_filterpolicy_create_bloom(10);
    context->options_ = rocksdb_options_create();
    rocksdb_options_set_max_open_files(context->options_, 10000);
    rocksdb_options_set_create_if_missing(context->options_, 1);
    rocksdb_options_set_create_missing_column_families(context->options_, 1);
    rocksdb_options_set_write_buffer_size(context->options_, write_buffer_size*1024*1024);
    rocksdb_options_set_block_based_table_factory(context->options_, context->table_options_);
    rocksdb_options_set_compression(context->options_, rocksdb_snappy_compression); 
    int num_column_families = num_slots;
    const char** column_family_names = slot_names;
    const rocksdb_options_t **column_family_options = (const rocksdb_options_t**)fdb_malloc(num_slots * sizeof(rocksdb_options_t*));
    for(int i=0; i<num_slots; ++i){
        column_family_options[i] = context->options_;
    }
    rocksdb_column_family_handle_t **column_family_handles = (rocksdb_column_family_handle_t**)malloc(num_slots * sizeof(rocksdb_column_family_handle_t*));
    
    char* rocksdb_error = NULL;
    context->db_ = rocksdb_open_column_families(context->options_, 
                                                name, 
                                                num_column_families, 
                                                column_family_names, 
                                                column_family_options, 
                                                column_family_handles, 
                                                &rocksdb_error); 
    if(rocksdb_error!=NULL){
        goto err;
    }
    for(int i=0; i<num_slots; ++i){
        slots[i]->handle_ = column_family_handles[i];
    }
    fdb_free(column_family_options);
    fdb_free(column_family_handles);
    return context;
err:
    if(context->db_!=NULL){
        rocksdb_close(context->db_);
    }
    if(column_family_options!=NULL){
        fdb_free(column_family_options);
    }
    if(column_family_handles!=NULL){
        fdb_free(column_family_handles);  
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
    if(context->filter_policy_!=NULL){
        rocksdb_filterpolicy_destroy(context->filter_policy_);
    }
    fdb_free(context);
    return NULL;
}

void fdb_context_destroy(fdb_context_t* context){
    if(context!=NULL){
        rocksdb_close(context->db_);
        rocksdb_cache_destroy(context->block_cache_);
        rocksdb_options_destroy(context->options_);
        rocksdb_block_based_options_destroy(context->table_options_);
        rocksdb_filterpolicy_destroy(context->filter_policy_); 
    }
    fdb_free(context); 
}

fdb_slot_t** fdb_slots_create(int num_slots){
    fdb_slot_t **slots = (fdb_slot_t**)fdb_malloc(num_slots * sizeof(fdb_slot_t*));
    for(int i=0; i<num_slots; ++i){
        slots[i] = (fdb_slot_t*)fdb_malloc(sizeof(fdb_slot_t));
        slots[i]->handle_ = NULL;
        slots[i]->keys_cache_ = rocksdb_cache_create_lru();
        slots[i]->batch_ = rocksdb_writebatch_create();
        slots[i]->mutex_ = rocksdb_mutex_create();
    }
    return slots;
}

void fdb_slots_destroy(int num_slots, fdb_slot_t** slots){
    for(int i=0; i<num_slots; ++i){
        if(slots[i]->handle_ !=NULL){
            rocksdb_column_family_handle_destroy(slots[i]->handle_);
        }
        rocksdb_cache_destroy(slots[i]->keys_cache_);
        rocksdb_writebatch_destroy(slots[i]->batch_);
        rocksdb_mutex_destroy(slots[i]->mutex_); 
        fdb_free(slots[i]);
    }
    fdb_free((void*)slots);
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
