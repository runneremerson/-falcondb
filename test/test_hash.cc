#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/fdb_types.h>
#include <falcondb/t_hash.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>


int main(int argc, char* argv[]){

    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_hash", 128, 128, 2);
    fdb_slot_t **slots = (fdb_slot_t**)ctx->slots_;

    fdb_context_drop_slot(ctx, slots[1]);
    fdb_context_create_slot(ctx, slots[1]);

    //hash_get hash_set
    fdb_slice_t* key1_0 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    fdb_slice_t* fld1 = fdb_slice_create("hash_fld1", strlen("hash_fld1"));
    fdb_slice_t* val1 = fdb_slice_create("hash_val1", strlen("hash_val1"));

    fdb_slice_t* get_val1 = NULL;
    int ret = hash_get(ctx, slots[1], key1_0, fld1, &get_val1);
    assert(ret == FDB_OK_NOT_EXIST);
    fdb_slice_destroy(key1_0);

    fdb_slice_t* key1_1 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    ret = hash_set(ctx, slots[1], key1_1, fld1, val1);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_1);

    fdb_slice_t* key1_2 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    ret = hash_get(ctx, slots[1], key1_2, fld1, &get_val1);
    fdb_slice_destroy(key1_2);

    assert(ret == FDB_OK);
    assert(memcmp(fdb_slice_data(val1), fdb_slice_data(get_val1), fdb_slice_length(val1))==0);
    fdb_slice_destroy(get_val1);


    //hash_del hash_exist
    fdb_slice_t* key1_3 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    ret = hash_exists(ctx, slots[1], key1_3, fld1);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_3);

    fdb_slice_t* key1_4 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    ret = hash_del(ctx, slots[1], key1_4, fld1);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_4);


    fdb_slice_t* key1_5 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    ret = hash_get(ctx, slots[1], key1_5, fld1, &get_val1);
    assert(ret == FDB_OK_NOT_EXIST);
    fdb_slice_destroy(key1_5);

    fdb_slice_t* key1_6 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    ret = hash_exists(ctx, slots[1], key1_6, fld1);
    assert(ret == FDB_OK_NOT_EXIST);
    fdb_slice_destroy(key1_6);


    fdb_slice_destroy(val1);
    fdb_slice_destroy(fld1);
    fdb_context_destroy(ctx);
    return 0;
}
