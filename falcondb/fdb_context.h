#ifndef FDB_CONTEXT_H
#define FDB_CONTEXT_H

#include <rocksdb/c.h>


struct fdb_context_t{
    rocksdb_t*                              db_;
    rocksdb_cache_t*                        block_cache_;
    rocksdb_options_t*                      options_;
    rocksdb_block_based_table_options_t*    table_options_;
    rocksdb_filterpolicy_t*                 filter_policy_;
};

struct fdb_slot_t{
    rocksdb_column_family_handle_t*      handle_; 
    rocksdb_writebatch_t*                batch_;
    rocksdb_mutex_t*                     mutex_;
};

typedef struct fdb_context_t    fdb_context_t;
typedef struct fdb_slot_t       fdb_slot_t;

//context
fdb_context_t* fdb_context_create(const char* name, size_t write_buffer_size, size_t cache_size, int num_slots, const char** slot_names, fdb_slot_t** slots);
void fdb_context_destroy(fdb_context_t* context);

//slot
fdb_slot_t** fdb_slots_create(int num_slots);
void fdb_slots_destroy(int num_slots, fdb_slot_t** slots);

//writebatch
void fdb_slot_writebatch_put(fdb_slot_t* slot, const char* key, size_t klen, const char* val, size_t vlen);
void fdb_slot_writebatch_delete(fdb_slot_t* slot, const char* key, size_t klen);
void fdb_slot_writebatch_commit(fdb_context_t* context, fdb_slot_t* slot, char** errptr);

#endif //FDB_CONTEXT_H
