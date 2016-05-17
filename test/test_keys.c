#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/t_keys.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char* argv[]){

    fdb_slot_t **slots = fdb_slots_create(2);
    const char* names[2] = {"default", "name2"};
    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_keys", 128, 128, 2, names, slots);

    fdb_slice_t* key1 = fdb_slice_create("keys_key1", strlen("keys_key1"));
    fdb_slice_t* val1 = fdb_slice_create("keys_val1", strlen("keys_val1"));
    //slot[0] string_set
    int ret = keys_set_string(ctx, slots[0], key1, val1);
    assert(ret == FDB_OK);


    //slot[1] string_set
    fdb_slice_t* val2 = fdb_slice_create("keys_val2", strlen("keys_val2"));
    ret = keys_set_string(ctx, slots[1], key1, val2);
    assert(ret == FDB_OK);

    
    //slot[0] string_get
    fdb_slice_t* val1_get = NULL;
    ret = keys_get_string(ctx, slots[0], key1, &val1_get);
    assert(ret == FDB_OK);
    assert(memcmp(fdb_slice_data(val1), fdb_slice_data(val1_get), fdb_slice_length(val1))==0);
    fdb_slice_destroy(val1_get);

    //slots[0] keys_del
    ret = keys_del(ctx, slots[0], key1);
    assert(ret == FDB_OK);
    ret = keys_get_string(ctx, slots[0], key1, &val1_get);
    assert(ret == FDB_OK_NOT_EXIST);


    //slot[1] string_get
    fdb_slice_t* val2_get = NULL;
    ret = keys_get_string(ctx, slots[1], key1, &val2_get);
    assert(ret == FDB_OK);
    assert(memcmp(fdb_slice_data(val2), fdb_slice_data(val2_get), fdb_slice_length(val2))==0);
    fdb_slice_destroy(val2_get);

    uint8_t type = 0;
    ret = keys_get(ctx, slots[1], key1, &type);
    assert(ret == FDB_OK);
    assert(type == FDB_DATA_TYPE_STRING);
    

    fdb_slice_destroy(key1);
    fdb_slice_destroy(val1);
    fdb_slice_destroy(val2);
    fdb_slots_destroy(slots, 2);
    fdb_context_destroy(ctx);
    return 0;
}


