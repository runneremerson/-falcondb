#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/t_string.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main(int argc, char* argv[]){

    fdb_slot_t **slots = fdb_slots_create(2);
    const char* names[2] = {"default", "name2"};
    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_string", 128, 128, 2, names, slots);

    fdb_slice_t *key1 = fdb_slice_create("string_key1", strlen("string_key1"));
    fdb_slice_t *val1 = fdb_slice_create("string_val1", strlen("string_val1"));

    //slots[0] set key1
    int ret = string_set(ctx, slots[0], key1, val1);
    assert(ret == FDB_OK);

    //slots[0] setnx key1
    fdb_slice_t *val2 = fdb_slice_create("string_val2", strlen("string_val2"));
    ret = string_setnx(ctx, slots[0], key1, val2);
    assert(ret == FDB_OK_BUT_ALREADY_EXIST);

    //slots[0] setxx key1
    fdb_slice_t *key2 = fdb_slice_create("string_key2", strlen("string_key2"));
    ret = string_setxx(ctx, slots[0], key2, val2);
    assert(ret == FDB_OK_NOT_EXIST);

    ret = string_setxx(ctx, slots[0], key1, val2);
    assert(ret == FDB_OK);

    //slot[0] get
    fdb_slice_t *get_val1 = NULL;
    ret = string_get(ctx, slots[0], key1, &get_val1);
    assert(ret == FDB_OK);
    assert(memcmp(fdb_slice_data(val2), fdb_slice_data(get_val1), fdb_slice_length(get_val1))==0);
    fdb_slice_destroy(get_val1);

    fdb_slice_t * key3 = fdb_slice_create("string_key3", strlen("string_key3"));
    ret = string_get(ctx, slots[0], key3, &get_val1);
    if(ret == FDB_OK_NOT_EXIST){
        int64_t nval = 0;
        assert(string_incr(ctx, slots[0], key3, 3333, 1, &nval) == FDB_OK);
        assert(nval == 3334);
    }else{
        int64_t old_nval = (int64_t)atoll(fdb_slice_data(get_val1));
        int64_t new_nval = 0;
        assert(string_incr(ctx, slots[0], key3, 3333, -1, &new_nval) == FDB_OK);
        assert((old_nval-1) == new_nval);
        fdb_slice_destroy(get_val1);
    }


    //slots[1] mset
    fdb_slice_t *val3 = fdb_slice_create("string_val3", strlen("string_val3"));
    fdb_array_t *kvs1 = fdb_array_create(4);
    //key1,val1
    fdb_val_node_t *n1_key1 = fdb_val_node_create();
    n1_key1->val_.vval_ = key1;
    fdb_val_node_t *n1_val1 = fdb_val_node_create();
    n1_val1->val_.vval_ = val1;

    fdb_array_push_back(kvs1, n1_key1);
    fdb_array_push_back(kvs1, n1_val1);

    //key2, val2
    fdb_val_node_t *n2_key2 = fdb_val_node_create();
    n2_key2->val_.vval_ = key2;
    fdb_val_node_t *n2_val2 = fdb_val_node_create();
    n2_val2->val_.vval_ = val2;

    fdb_array_push_back(kvs1, n2_key2);
    fdb_array_push_back(kvs1, n2_val2);

    //key3, val3 
    fdb_val_node_t *n3_key3 = fdb_val_node_create();
    n3_key3->val_.vval_ = key3;
    fdb_val_node_t *n3_val3 = fdb_val_node_create();
    n3_val3->val_.vval_ = val3;

    fdb_array_push_back(kvs1, n3_key3);
    fdb_array_push_back(kvs1, n3_val3);

    //mset
    fdb_array_t *val_rets = NULL;
    ret = string_mset(ctx, slots[1], kvs1, &val_rets);
    assert(ret == FDB_OK);
    for(int i=0; i<val_rets->length_; ++i){
        fdb_val_node_t *node = fdb_array_at(val_rets, i);
        assert(node->retval_==FDB_OK);
    }
    fdb_array_destroy(val_rets);

    
    //slots[1] mget
    fdb_array_t *keys = fdb_array_create(4);
    fdb_array_push_back(keys, n1_key1);
    fdb_array_push_back(keys, n2_key2);
    fdb_array_push_back(keys, n3_key3);
    fdb_slice_t *key4_nx = fdb_slice_create("string_key4_nx", strlen("string_key4_nx"));
    fdb_val_node_t *n4_key4 = fdb_val_node_create();
    n4_key4->val_.vval_ = key4_nx;
    fdb_array_push_back(keys, n4_key4);
    ret = string_mget(ctx, slots[1], keys, &val_rets);
    assert(ret == FDB_OK);
    for(int i=0; i<val_rets->length_; ++i){
        fdb_val_node_t *node = fdb_array_at(val_rets, i);
        if(i<3){
            assert(node->retval_== FDB_OK); 
        }else{
            assert(node->retval_ == FDB_OK_NOT_EXIST);
        }
    }
    assert(val_rets->length_ == 4);
    assert(fdb_array_at(val_rets, 3)->val_.vval_ == NULL);

    fdb_slice_t *val3_ret = (fdb_slice_t*)(fdb_array_at(val_rets, 2)->val_.vval_);
    assert(memcmp(fdb_slice_data(val3_ret), fdb_slice_data(val3), fdb_slice_length(val3))==0);

    fdb_slice_t *val2_ret = (fdb_slice_t*)(fdb_array_at(val_rets, 1)->val_.vval_);
    assert(memcmp(fdb_slice_data(val2_ret), fdb_slice_data(val2), fdb_slice_length(val2))==0);

    fdb_slice_t *val1_ret = (fdb_slice_t*)(fdb_array_at(val_rets, 0)->val_.vval_);
    assert(memcmp(fdb_slice_data(val1_ret), fdb_slice_data(val1), fdb_slice_length(val1))==0);

    fdb_array_destroy(val_rets);


    //slots[1] msetnx
    fdb_slice_t *key4 = fdb_slice_create("string_key4", strlen("string_key4"));
    fdb_slice_t *val4 = fdb_slice_create("string_val4", strlen("string_val4"));
    //key4, val4 
    n4_key4 = fdb_val_node_create();
    n4_key4->val_.vval_ = key4;
    fdb_val_node_t *n4_val4 = fdb_val_node_create();
    n4_val4->val_.vval_ = val4;

    fdb_array_push_back(kvs1, n4_key4);
    fdb_array_push_back(kvs1, n4_val4);

    ret = string_msetnx(ctx, slots[1], kvs1, &val_rets);
    assert(ret == FDB_OK);
    for(int i=0; i<val_rets->length_; ++i){
        fdb_val_node_t *node = fdb_array_at(val_rets, i);
        if(i<3){
            assert(node->retval_==FDB_OK_BUT_ALREADY_EXIST);    
        }else{
            assert(node->retval_==FDB_OK || node->retval_ == FDB_OK_BUT_ALREADY_EXIST);
        }
    }
    fdb_array_destroy(val_rets);

    //slots[1] mget
    fdb_val_node_t *n_back = fdb_array_back(keys);
    fdb_val_node_destroy(n_back);

    fdb_array_pop_back(keys);
    fdb_array_push_back(keys, n4_key4);
    ret = string_mget(ctx, slots[1], keys, &val_rets);
    assert(ret == FDB_OK);
    for(int i=0; i<val_rets->length_; ++i){
        fdb_val_node_t *node = fdb_array_at(val_rets, i);
        assert(node->retval_== FDB_OK); 
    }
    assert(val_rets->length_ == 4);

    fdb_slice_t *val4_ret = (fdb_slice_t*)(fdb_array_at(val_rets, 3)->val_.vval_);
    assert(memcmp(fdb_slice_data(val4_ret), fdb_slice_data(val4), fdb_slice_length(val4))==0);

    fdb_array_destroy(val_rets);
    



    fdb_slice_destroy(key1);
    fdb_slice_destroy(key2);
    fdb_slice_destroy(key3);
    fdb_slice_destroy(key4);
    fdb_slice_destroy(key4_nx);
    fdb_slice_destroy(val1);
    fdb_slice_destroy(val2);
    fdb_slice_destroy(val3);
    fdb_slice_destroy(val4);
    fdb_array_destroy(kvs1);
    fdb_array_destroy(keys);
    fdb_slots_destroy(slots, 2);
    fdb_context_destroy(ctx);
    return 0;
}

