#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/fdb_types.h>
#include <falcondb/t_set.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

void print_set(fdb_context_t* ctx, fdb_slot_t* slot, fdb_slice_t* key){
    fdb_array_t *set = fdb_array_create(8);
    int ret = set_members(ctx, slot, key, &set);
    assert(ret == FDB_OK);
    for(int i=0; i<set->length_; ++i){
        fdb_val_node_t *node = fdb_array_at(set, i);
        fdb_slice_t *sl = (fdb_slice_t*)(node->val_.vval_);
        fprintf(stdout, "[%lu] %s\n",set->length_, fdb_slice_data(sl));
        fdb_slice_destroy(sl); 
    }
    fdb_array_destroy(set);
}

int main(int argc, char* argv[]){
    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_set", 128, 128, 2);
    fdb_slot_t **slots = (fdb_slot_t**)ctx->slots_;

    fdb_context_drop_slot(ctx, slots[1]);
    fdb_context_create_slot(ctx, slots[1]);

    /*
    fdb_slice_t *key1_0 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber1 = fdb_slice_create("member1", strlen("member1"));
    int ret = set_add(ctx, slots[1], key1_0, mber1);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_0);


    fdb_slice_t *key1_0_0 = fdb_slice_create("key1", strlen("key1"));
    int64_t size = -1;
    ret = set_size(ctx, slots[1], key1_0_0, &size);
    assert(ret == FDB_OK);
    assert(size == 1);
    fdb_slice_destroy(key1_0_0);

    fdb_slice_t *key1_0_1 = fdb_slice_create("key1", strlen("key1"));
    ret = set_rem(ctx, slots[1], key1_0_1, mber1);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_0_1);

    fdb_slice_t *key1_1 = fdb_slice_create("key1", strlen("key1"));
    size = -1;
    ret = set_size(ctx, slots[1], key1_1, &size);
    assert(ret == FDB_OK);
    assert(size == 0);
    fdb_slice_destroy(key1_1);



    fdb_slice_t *key1_1_0 = fdb_slice_create("key1", strlen("key1"));
    ret = set_add(ctx, slots[1], key1_1_0, mber1);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_1_0);



    fdb_slice_t *key1_2 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber2 = fdb_slice_create("member2", strlen("member2"));
    ret = set_add(ctx, slots[1], key1_2, mber2);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_2);


    fdb_slice_t *key1_2_0 = fdb_slice_create("key1", strlen("key1"));
    ret = set_add(ctx, slots[1], key1_2_0, mber2);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_2_0);

    fdb_slice_t *key1_2_1 = fdb_slice_create("key1", strlen("key1"));
    ret = set_member_exists(ctx, slots[1], key1_2_1, mber2);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_2_1);


    fdb_slice_t *mberx = fdb_slice_create("memberx", strlen("memberx"));
    fdb_slice_t *key1_2_2 = fdb_slice_create("key1", strlen("key1"));
    ret = set_member_exists(ctx, slots[1], key1_2_2, mberx);
    assert(ret == FDB_OK_NOT_EXIST);
    fdb_slice_destroy(key1_2_2);


    fdb_slice_t *key1_3 = fdb_slice_create("key1", strlen("key1"));
    size = -1;
    ret = set_size(ctx, slots[1], key1_3, &size);
    assert(ret == FDB_OK);
    assert(size == 2);
    fdb_slice_destroy(key1_3);


    fdb_slice_t *mber3 = fdb_slice_create("member3", strlen("member3"));
    fdb_slice_t *key1_4 = fdb_slice_create("key1", strlen("key1"));
    ret = set_add(ctx, slots[1], key1_4, mber3);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_4);



    fdb_slice_t *mber3_0 = fdb_slice_create("member30", strlen("member30"));
    fdb_slice_t *key1_4_0 = fdb_slice_create("key1", strlen("key1"));
    ret = set_add(ctx, slots[1], key1_4_0, mber3_0);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_4_0);


    fdb_slice_t *mber3_1 = fdb_slice_create("member31", strlen("member31"));
    fdb_slice_t *key1_4_1 = fdb_slice_create("key1", strlen("key1"));
    ret = set_add(ctx, slots[1], key1_4_1, mber3_1);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_4_1);

    fdb_slice_t *mber4 = fdb_slice_create("member4", strlen("member4"));
    fdb_slice_t *key1_5_0 = fdb_slice_create("key1", strlen("key1"));
    ret = set_add(ctx, slots[1], key1_5_0, mber4);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_5_0);


    fdb_slice_t *key1_6 = fdb_slice_create("key1", strlen("key1"));
    size = -1;
    ret = set_size(ctx, slots[1], key1_6, &size);
    assert(ret == FDB_OK);
    assert(size == 6);
    fdb_slice_destroy(key1_6);

    fdb_slice_t *key1_7 = fdb_slice_create("key1", strlen("key1"));
    print_set(ctx, slots[1], key1_7);
    fdb_slice_destroy(key1_7);


    fdb_slice_t *key1_8 = fdb_slice_create("key1", strlen("key1"));
    ret = set_rem(ctx, slots[1], key1_8, mber1);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_8);


    fdb_slice_t *key1_9 = fdb_slice_create("key1", strlen("key1"));
    ret = set_rem(ctx, slots[1], key1_9, mber1);
    assert(ret == FDB_OK_NOT_EXIST);
    fdb_slice_destroy(key1_9);


    fdb_slice_t *key1_10 = fdb_slice_create("key1", strlen("key1"));
    print_set(ctx, slots[1], key1_10);
    fdb_slice_destroy(key1_10);


    fdb_slice_t *key1_11 = fdb_slice_create("key1", strlen("key1"));
    size = -1;
    ret = set_size(ctx, slots[1], key1_11, &size);
    assert(ret == FDB_OK);
    assert(size == 5);
    fdb_slice_destroy(key1_11);
    */


    fdb_context_destroy(ctx);
    return 0;
}

