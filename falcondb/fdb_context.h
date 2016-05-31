#ifndef FDB_CONTEXT_H
#define FDB_CONTEXT_H

#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct fdb_context_t                fdb_context_t;
typedef struct fdb_slot_t                   fdb_slot_t;

//database
extern void fdb_drop_db(const char* name);

//context
extern fdb_context_t* fdb_context_create(const char* name, size_t write_buffer_size, size_t cache_size, size_t num_slots);
extern void fdb_context_destroy(fdb_context_t* context);
extern void fdb_context_drop_slot(fdb_context_t* context, fdb_slot_t* slot);
extern void fdb_context_create_slot(fdb_context_t* context, fdb_slot_t* slot);

//writebatch
extern void fdb_slot_writebatch_put(fdb_slot_t* slot, const char* key, size_t klen, const char* val, size_t vlen);
extern void fdb_slot_writebatch_delete(fdb_slot_t* slot, const char* key, size_t klen);
extern void fdb_slot_writebatch_commit(fdb_context_t* context, fdb_slot_t* slot, char** errptr);

#ifdef __cplusplus
}
#endif

#endif //FDB_CONTEXT_H
