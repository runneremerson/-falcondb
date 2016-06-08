#ifndef FDB_ITERATOR_H
#define FDB_ITERATOR_H

#include "fdb_context.h"
#include "fdb_slice.h"

#include <stdint.h>
#include <stddef.h>

enum {
    FORWARD = 0,
    BACKWARD = 1
};

typedef struct fdb_iterator_t       fdb_iterator_t;

fdb_iterator_t* fdb_iterator_create(fdb_context_t* context, fdb_slot_t* slot, fdb_slice_t* start,
                                    fdb_slice_t* end, uint64_t limit, int direction);

void fdb_iterator_destroy(fdb_iterator_t* iterator);

int fdb_iterator_skip(fdb_iterator_t* iterator, uint64_t offset);

int fdb_iterator_valid(const fdb_iterator_t* iterator);

int fdb_iterator_next(fdb_iterator_t* iterator);

void fdb_iterator_val(const fdb_iterator_t* iterator, fdb_slice_t** pslice);
void fdb_iterator_key(const fdb_iterator_t* iterator, fdb_slice_t** pslice);

const char* fdb_iterator_val_raw(const fdb_iterator_t *iterator, size_t* vlen);
const char* fdb_iterator_key_raw(const fdb_iterator_t *iterator, size_t* klen);
#endif //FDB_ITERATOR_H
