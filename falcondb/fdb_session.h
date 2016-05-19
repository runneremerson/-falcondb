#ifndef FDB_SESSION_H
#define FDB_SESSION_H

#include "fdb_context.h"
#include "fdb_slice.h"
#include <stdint.h>

typedef struct fdb_item_t{
    uint64_t data_len_;
    char* data_;
    void* hold_;
    int64_t retval_;
} fdb_item_t;


void free_fdb_item(fdb_item_t* item);

void destroy_fdb_item_array(fdb_item_t* array, size_t size);

fdb_item_t* create_fdb_item_array( size_t size);

int* create_int_array(size_t size);

void free_int_array(void* array);


int set_fdb_signal_handler(const char* name);


int fdb_set(fdb_context_t* context,
            uint64_t id,
            fdb_item_t* key,
            fdb_item_t* val,
            int64_t duration, 
            int en,
            int* ef);

int fdb_mset(fdb_context_t* context,
             uint64_t id,
             size_t length,
             fdb_item_t* kvs,
             int** rets,
             int en);

int fdb_get(fdb_context_t* context, 
            uint64_t id,
            fdb_item_t* key,
            fdb_item_t** pval);

int fdb_mget(fdb_context_t* context,
             uint64_t id,
             size_t length,
             fdb_item_t* keys,
             fdb_item_t** vals);


int fdb_del(fdb_context_t* context, 
            uint64_t id,
            fdb_item_t* key);

int fdb_incrby(fdb_context_t* context,
               uint64_t id,
               fdb_item_t* key,
               int64_t init,
               int64_t by,
               int64_t *result);

#endif //FDB_SESSION_H

