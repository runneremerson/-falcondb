#ifndef FDB_SESSION_H
#define FDB_SESSION_H

#include "fdb_context.h"
#include <stdint.h>
#include <stdlib.h>


#ifdef __cplusplus
extern "C" {
#endif

typedef struct fdb_item_t{
    uint64_t data_len_;
    char* data_;
    void* hold_;
    int64_t retval_;
} fdb_item_t;


extern void free_fdb_item(fdb_item_t* item);

extern void destroy_fdb_item_array(fdb_item_t* array, size_t size);

extern fdb_item_t* create_fdb_item_array( size_t size);

extern int* create_int_array(size_t size);

extern void free_int_array(void* array);


extern int set_fdb_signal_handler(const char* name);

extern void fdb_drop_slot(fdb_context_t* context, uint64_t id);

extern int fdb_set(fdb_context_t* context,
            uint64_t id,
            fdb_item_t* key,
            fdb_item_t* val,
            int64_t duration, 
            int en,
            int* ef);

extern int fdb_mset(fdb_context_t* context,
             uint64_t id,
             size_t length,
             fdb_item_t* kvs,
             int** rets,
             int en);

extern int fdb_get(fdb_context_t* context, 
            uint64_t id,
            fdb_item_t* key,
            fdb_item_t** pval);

extern int fdb_mget(fdb_context_t* context,
             uint64_t id,
             size_t length,
             fdb_item_t* keys,
             fdb_item_t** vals);


extern int fdb_del(fdb_context_t* context, 
            uint64_t id,
            fdb_item_t* key);

extern int fdb_incrby(fdb_context_t* context,
               uint64_t id,
               fdb_item_t* key,
               int64_t init,
               int64_t by,
               int64_t *result);


#ifdef __cplusplus
}
#endif

#endif //FDB_SESSION_H

