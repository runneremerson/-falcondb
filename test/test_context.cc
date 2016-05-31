#include <falcondb/fdb_types.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/fdb_malloc.h>
#include <falcondb/fdb_slice.h>
#include <falcondb/t_string.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>


int main(int argc, char* argv[]){
    fdb_drop_db("/tmp/falcondb_test_context");
    fdb_context_t *ctx = fdb_context_create("/tmp/falcondb_test_context", 16, 32, 5);

    fdb_slot_t **slots = (fdb_slot_t**)ctx->slots_;
    fdb_context_drop_slot(ctx, slots[1]);
    fdb_context_create_slot(ctx, slots[1]);


    fdb_slice_t *key1 = fdb_slice_create("string_key1", strlen("string_key1"));
    fdb_slice_t *get_val1 = NULL;

    int ret = string_get(ctx, slots[0], key1, &get_val1);
    assert(ret == FDB_OK_NOT_EXIST);

    fdb_slice_t *val1 = fdb_slice_create("string_val1", strlen("string_val1"));

    ret = string_set(ctx, slots[1], key1, val1);
    assert(ret == FDB_OK);


    ret = string_get(ctx, slots[1], key1, &get_val1);
    assert(ret == FDB_OK);
    assert(memcmp(fdb_slice_data(get_val1), fdb_slice_data(val1), fdb_slice_length(get_val1))==0);

    fdb_slice_destroy(get_val1);
    fdb_slice_destroy(key1);
    fdb_slice_destroy(val1);
    fdb_context_destroy(ctx);
    return 0;
}
