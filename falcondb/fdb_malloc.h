#ifndef FDB_MALLOC_H
#define FDB_MALLOC_H


void* fdb_malloc(size_t size);
void* fdb_realloc(void *ptr, size_t size);
void  fdb_free(void *ptr);
char* fdb_strdup(const char *s);

#endif //FDB_MALLOC_H
