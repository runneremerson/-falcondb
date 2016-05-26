#include "fdb_malloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "config.h"


#if defined(USE_TCMALLOC)
#define malloc(size) tc_malloc(size)
#define calloc(count,size) tc_calloc(count,size)
#define realloc(ptr,size) tc_realloc(ptr,size)
#define free(ptr) tc_free(ptr)
#endif

#if defined(USE_JEMALLOC)
#define malloc(size) je_malloc(size)
#define calloc(count,size) je_calloc(count,size)
#define realloc(ptr,size) je_realloc(ptr,size)
#define free(ptr) je_free(ptr)
#endif

void fdb_incr_ref_count(void* ptr){
    fdb_ref_t *ref = (fdb_ref_t*)ptr;
    ref->refcnt_ += 1;
}

static void fdb_malloc_oom(size_t size) {
    fprintf(stderr, "fdb_malloc: Out of memory trying to allocate %zu bytes\n",
        size);
    fflush(stderr);
    abort();
}


void *fdb_malloc(size_t size) {
    void *ptr = malloc(size);

    if (!ptr) {
        // try again
        ptr = malloc(size);
        if (!ptr)
            fdb_malloc_oom(size);
    }
    return ptr;
}


void *fdb_realloc(void *ptr, size_t size) {
    void *newptr;

    if (ptr == NULL) return fdb_malloc(size);

    newptr = realloc(ptr,size);
    if (!newptr) fdb_malloc_oom(size);

    return newptr;
}

void fdb_free(void *ptr) {
    if (ptr == NULL) return;
    free(ptr);
}

char *fdb_strdup(const char *s) {
    size_t len = strlen(s);
    char *p = (char*)fdb_malloc(len+1);
    memcpy(p,s,len);
    p[len] = '\0';
    return p;
}

char *fdb_strdup_with_length(const char *s, size_t len) {
    char *p = (char*)fdb_malloc(len+1);
    memcpy(p,s,len);
    p[len] = '\0';
    return p;
}
