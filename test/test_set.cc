#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/fdb_types.h>
#include <falcondb/t_set.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

void print_set(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
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
    fdb_slice_destroy(key);
}

void test_set_add(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* smember, int retcode, int64_t count){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t *mber = fdb_slice_create(smember, strlen(smember));
    
    fdb_array_t *members = fdb_array_create(2);
    fdb_val_node_t *member_node = fdb_val_node_create();
    member_node->val_.vval_ = mber;

    fdb_array_push_back(members, member_node);

    int64_t add_count = 0;
    int ret = set_add(ctx, slot, key, members, &add_count);
    assert(retcode == ret);
    assert(add_count == count);

    fdb_slice_destroy(key);
    fdb_slice_destroy(mber);
}

void test_set_size(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, int retcode, int64_t size){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    int64_t set_size_ = -1;
    int ret = set_size(ctx, slot, key, &set_size_);
    assert(retcode == ret);
    assert(size == set_size_);
    fdb_slice_destroy(key);
}

void test_set_rem(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* smember, int retcode, int64_t count){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey)); 
    fdb_slice_t *mber = fdb_slice_create(smember, strlen(smember));
    fdb_val_node_t *member_node = fdb_val_node_create();
    member_node->val_.vval_ = mber;
    fdb_array_t *members = fdb_array_create(2);
    fdb_array_push_back(members, member_node);
    int64_t rem_count = 0;
    int ret = set_rem(ctx, slot, key, members, &rem_count);
    assert(ret == retcode);
    assert(count == rem_count);
    fdb_slice_destroy(key);
    fdb_slice_destroy(mber);
    fdb_array_destroy(members);
}

void test_set_member_exists(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* smember, int retcode, int64_t count){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey)); 
    fdb_slice_t *mber = fdb_slice_create(smember, strlen(smember));
    int64_t exists_count = -1;
    int ret = set_member_exists(ctx, slot, key, mber, &exists_count);
    assert(ret == retcode);
    if(ret == FDB_OK){
        assert(count == exists_count);
    }
    fdb_slice_destroy(key);
    fdb_slice_destroy(mber);
}

int main(int argc, char* argv[]){
    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_set", 128, 128, 2);
    fdb_slot_t **slots = (fdb_slot_t**)ctx->slots_;

    fdb_context_drop_slot(ctx, slots[1]);
    fdb_context_create_slot(ctx, slots[1]);


    test_set_add(ctx, slots[1], "key1", "member1", FDB_OK, 1);

    test_set_size(ctx, slots[1], "key1", FDB_OK, 1);

    test_set_rem(ctx, slots[1], "key1", "member1", FDB_OK, 1);

    test_set_size(ctx, slots[1], "key1", FDB_OK, 0);

    test_set_add(ctx, slots[1], "key1", "member1", FDB_OK, 1);

    test_set_add(ctx, slots[1], "key1", "member2", FDB_OK, 1);

    test_set_add(ctx, slots[1], "key1", "member2", FDB_OK, 0);

    test_set_member_exists(ctx, slots[1], "key1", "member2", FDB_OK, 1);

    test_set_member_exists(ctx, slots[1], "key1", "memberx", FDB_OK, 0);

    test_set_size(ctx, slots[1], "key1", FDB_OK, 2);

    test_set_add(ctx, slots[1], "key1", "member3", FDB_OK, 1);

    test_set_add(ctx, slots[1], "key1", "member30", FDB_OK, 1);

    test_set_add(ctx, slots[1], "key1", "member31", FDB_OK, 1);

    test_set_add(ctx, slots[1], "key1", "member4", FDB_OK, 1);

    test_set_size(ctx, slots[1], "key1", FDB_OK, 6);

    print_set(ctx, slots[1], "key1");

    test_set_rem(ctx, slots[1], "key1", "member1", FDB_OK, 1);

    test_set_rem(ctx, slots[1], "key1", "member1", FDB_OK, 0);

    print_set(ctx, slots[1], "key1");
    
    test_set_size(ctx, slots[1], "key1", FDB_OK, 5);

    fdb_context_destroy(ctx);
    return 0;
}

