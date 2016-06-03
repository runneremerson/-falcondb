#ifndef FDB_TYPES_H
#define FDB_TYPES_H

#include <string.h>
#include <stdint.h>
#include <rocksdb/c.h>


struct fdb_context_t{
    rocksdb_t*                              db_;
    void*                                   slots_;
    size_t                                  num_slots_;
    rocksdb_cache_t*                        block_cache_;
    rocksdb_options_t*                      options_;
    rocksdb_block_based_table_options_t*    table_options_;
};

struct fdb_slot_t{
    uint64_t                                id_;
    rocksdb_column_family_handle_t*         handle_; 
    rocksdb_cache_t*                        keys_cache_;
    rocksdb_writebatch_t*                   batch_;
    rocksdb_mutex_t*                        mutex_; 
};


struct fdb_val_node_t {
    struct fdb_val_node_t* next_;
    struct fdb_val_node_t* prev_;
    union val_t {
        double      dval_;
        uint64_t    uval_;
        void*       vval_;
    } val_;
    int retval_;
};

struct fdb_list_t {
    struct fdb_val_node_t *head_;
    struct fdb_val_node_t *tail_;
    size_t length_;
};

struct fdb_array_t {
    struct fdb_val_node_t **array_;
    size_t length_;
    size_t capacity_;
};

struct fdb_iter_t {
    struct fdb_val_node_t *curr_;
    size_t length_;
    size_t now_; 
};

struct fdb_iterator_t {
    struct fdb_slice_t *end_;
    int direction_;
    uint64_t limit_;
    rocksdb_iterator_t *iterator_;
};

#endif //FDB_TYPES_H
