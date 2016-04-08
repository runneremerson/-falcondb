#ifndef FDB_SESSION_H
#define FDB_SESSION_H

#include "fdb_context.h"
#include <stdint.h>

typedef struct fdb_item_t{
    size_t data_len_;
    char* data_;
} fdb_item_t;


void fill_fdb_item(fdb_item_t* item, const char* data, size_t len);

void free_fdb_item(fdb_item_t* item);

void destroy_fdb_item_array(fdb_item_t* array, size_t size);

fdb_item_t* create_fdb_item_array( size_t size);


int set_fdb_signal_handler(const char* name);


int fdb_set(fdb_context_t* context,
            fdb_slot_t* slot,
            fdb_item_t* key,
            fdb_item_t* val,
            uint64_t exptime, 
            int en);

int fdb_get(fdb_context_t* context, 
            fdb_slot_t* slot,
            fdb_item_t* key,
            fdb_item_t** pval);

int fdb_del(fdb_context_t* context, 
            fdb_slot_t* slot,
            fdb_item_t* key);

#endif //FDB_SESSION_H

