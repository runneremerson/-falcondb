#include "fdb_types.h"
#include "fdb_iterator.h"
#include "fdb_malloc.h"

#include "util.h"

#include <string.h>
#include <stdio.h>


fdb_iterator_t* fdb_iterator_create(fdb_context_t* context, fdb_slot_t* slot,
                                    fdb_slice_t* key, fdb_slice_t* start,
                                    fdb_slice_t* end, uint64_t limit,
                                    int direction){
    fdb_iterator_t *iterator = (fdb_iterator_t*)fdb_malloc(sizeof(fdb_iterator_t));
    iterator->key_ = fdb_slice_create(fdb_slice_data(key), fdb_slice_length(key));
    iterator->end_ = fdb_slice_create(fdb_slice_data(end), fdb_slice_length(end));
    iterator->direction_ = direction;
    iterator->limit_ = limit;
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    rocksdb_readoptions_set_fill_cache(readoptions, 0);
    iterator->iterator_ = rocksdb_create_iterator_cf(context->db_, readoptions, slot->handle_);
    rocksdb_iter_seek(iterator->iterator_, fdb_slice_data(start), fdb_slice_length(start));

    if(iterator->direction_ == FORWARD){
        if(rocksdb_iter_valid(iterator->iterator_)){
            size_t klen = 0;
            const char* key = rocksdb_iter_key(iterator->iterator_, &klen);
            if(compare_with_length(fdb_slice_data(start), fdb_slice_length(start), key, klen)==0){
                rocksdb_iter_next(iterator->iterator_);
            } 
        }
    }else{
        if(rocksdb_iter_valid(iterator->iterator_)){
            rocksdb_iter_prev(iterator->iterator_);
        }else{
            rocksdb_iter_seek_to_last(iterator->iterator_);
        }
    }

    rocksdb_readoptions_destroy(readoptions);
    return iterator;
}

void fdb_iterator_destroy(fdb_iterator_t* iterator){
    if(iterator!=NULL){
        fdb_slice_destroy(iterator->key_);
        fdb_slice_destroy(iterator->end_);
        rocksdb_iter_destroy(iterator->iterator_);
    }
    fdb_free(iterator);
}

int fdb_iterator_next(fdb_iterator_t* iterator){
    
    int ret = 0;

    while(1){

        if(iterator->direction_ == FORWARD){
            rocksdb_iter_next(iterator->iterator_);
        }else{
            rocksdb_iter_prev(iterator->iterator_);
        }

        if(iterator->limit_ == 0){
            ret = -1;
            goto end;
        }

        if(!rocksdb_iter_valid(iterator->iterator_)){
            iterator->limit_ = 0;
            ret = -1;
            goto end;
        }

        size_t klen = 0;
        const char *key = rocksdb_iter_key(iterator->iterator_, &klen);
        if(iterator->direction_ == FORWARD){
            if(fdb_slice_length(iterator->end_) > 0){
                if(compare_with_length(key, klen, fdb_slice_data(iterator->end_), fdb_slice_length(iterator->end_)) >= 0){
                    iterator->limit_ = 0;
                    ret = -1;
                    goto end;
                }
            }
        }else{
            if(fdb_slice_length(iterator->end_) >0){
                if(compare_with_length(key, klen, fdb_slice_data(iterator->end_), fdb_slice_length(iterator->end_)) <= 0){
                    iterator->limit_ = 0;
                    ret = -1;
                    goto end;
                }
            }
        }
        (iterator->limit_)--;

        ret = 0;
        goto end;
    }
  
end:
    return ret;
}


int fdb_iterator_skip(fdb_iterator_t* iterator, uint64_t offset){ 
    int ret = 0;
    while(offset>0){
        if(fdb_iterator_next(iterator) < 0){
            ret = -1;
            goto end;
        }
        --offset;
    }
    ret = 0;

end:
    return ret;
}

void fdb_iterator_val(const fdb_iterator_t* iterator, fdb_slice_t** pslice){
    size_t vlen = 0;
    const char *val = rocksdb_iter_value(iterator->iterator_, &vlen);
    *pslice = fdb_slice_create(val, vlen); 
}


void fdb_iterator_key(const fdb_iterator_t* iterator, fdb_slice_t** pslice){ 
    size_t klen = 0;
    const char *key = rocksdb_iter_key(iterator->iterator_, &klen);
    *pslice = fdb_slice_create(key, klen);
}

const char* fdb_iterator_key_raw(const fdb_iterator_t *iterator, size_t* klen){
    return rocksdb_iter_key(iterator->iterator_, klen);
}

const char* fdb_iterator_val_raw(const fdb_iterator_t *iterator, size_t* vlen){
    return rocksdb_iter_value(iterator->iterator_, vlen);
}
