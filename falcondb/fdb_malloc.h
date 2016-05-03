#ifndef FDB_MALLOC_H
#define FDB_MALLOC_H

#include <stdlib.h>

struct fdb_ref_t {
    size_t refcnt_;
};


typedef struct fdb_ref_t  fdb_ref_t;

void  fdb_incr_ref_count(void* ptr);
void* fdb_malloc(size_t size);
void* fdb_realloc(void *ptr, size_t size);
void  fdb_free(void *ptr);
char* fdb_strdup(const char *s);
char* fdb_strdup_with_length(const char *s, size_t len);

#endif //FDB_MALLOC_H
