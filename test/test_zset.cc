#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/fdb_types.h>
#include <falcondb/t_zset.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>


int main(int argc, char* argv[]){
    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_zset", 128, 128, 2);
    fdb_slot_t **slots = (fdb_slot_t**)ctx->slots_;

    fdb_context_drop_slot(ctx, slots[1]);
    fdb_context_create_slot(ctx, slots[1]);

    fdb_slice_t *key1_0 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber1 = fdb_slice_create("member1", strlen("member1"));
    int ret = zset_add(ctx, slots[1], key1_0, mber1, 1.0048);
    assert(ret == FDB_OK);


    fdb_slice_t *key1_1 = fdb_slice_create("key1", strlen("key1"));
    ret = zset_add(ctx, slots[1], key1_1, mber1, 1.2233);
    assert(ret == FDB_OK);


    fdb_slice_t *key1_2 = fdb_slice_create("key1", strlen("key1"));
    int64_t size = 0;
    ret = zset_size(ctx, slots[1], key1_2, &size);
    assert(ret == FDB_OK);
    assert(size == 2);

    return 0;
}
