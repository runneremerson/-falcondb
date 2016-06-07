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


    fdb_slice_t* key1_7 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    ret = hash_setnx(ctx, slots[1], key1_7, fld1, val1);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_7);

    fdb_slice_t* key1_8 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    ret = hash_setnx(ctx, slots[1], key1_8, fld1, val1);
    assert(ret == FDB_OK_BUT_ALREADY_EXIST);
    fdb_slice_destroy(key1_8);

    fdb_slice_t* key1_9 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    fdb_slice_t* fld2 = fdb_slice_create("hash_fld2", strlen("hash_fld2"));
    int64_t val = 0;
    ret = hash_incr(ctx, slots[1], key1_9, fld2, 0, 98, &val);
    assert(ret == FDB_OK);
    assert(val == 98);
    fdb_slice_destroy(key1_9);

    fdb_slice_t* key1_10 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    val = 0;
    ret = hash_incr(ctx, slots[1], key1_9, fld2, 0, -1, &val);
    assert(ret == FDB_OK);
    assert(val == 97);
    fdb_slice_destroy(key1_10);


    fdb_slice_t* key1_11 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    int64_t length = 0;
    ret = hash_length(ctx, slots[1], key1_11, &length);
    assert(ret == FDB_OK);
    assert(length == 2);
    fdb_slice_destroy(key1_11);


    //all
    fdb_slice_t* key1_12 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    fdb_array_t *fvs_rets1 = NULL;
    ret = hash_getall(ctx, slots[1], key1_12, &fvs_rets1);
    assert(ret == FDB_OK);
    assert(fvs_rets1->length_ == 4);
    for(int i=0; i<fvs_rets1->length_; ++i){ 
        fdb_val_node_t *node = fdb_array_at(fvs_rets1, i);
        fdb_slice_t *sl = (fdb_slice_t*)(node->val_.vval_);
        fprintf(stdout, "%s\n", fdb_slice_data(sl));
        fdb_slice_destroy(sl);
    }
    fdb_array_destroy(fvs_rets1);
    fdb_slice_destroy(key1_12);

    //keys
    fprintf(stdout, "hkeys\n");
    fdb_slice_t* key1_13 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    fdb_array_t *keys_rets1 = NULL;
    ret = hash_keys(ctx, slots[1], key1_13, &keys_rets1);
    assert(ret == FDB_OK);
    assert(keys_rets1->length_ == 2);
    for(int i=0; i<keys_rets1->length_; ++i){ 
        fdb_val_node_t *node = fdb_array_at(keys_rets1, i);
        fdb_slice_t *sl = (fdb_slice_t*)(node->val_.vval_);
        fprintf(stdout, "%s\n", fdb_slice_data(sl));
        fdb_slice_destroy(sl);
    }
    fdb_array_destroy(keys_rets1);
    fdb_slice_destroy(key1_13);


    //vals 
    fprintf(stdout, "hvals\n");
    fdb_slice_t* key1_14 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    fdb_array_t *vals_rets1 = NULL;
    ret = hash_vals(ctx, slots[1], key1_14, &vals_rets1);
    assert(ret == FDB_OK);
    assert(vals_rets1->length_ == 2);
    for(int i=0; i<vals_rets1->length_; ++i){ 
        fdb_val_node_t *node = fdb_array_at(vals_rets1, i);
        fdb_slice_t *sl = (fdb_slice_t*)(node->val_.vval_);
        fprintf(stdout, "%s\n", fdb_slice_data(sl));
        fdb_slice_destroy(sl);
    }
    fdb_array_destroy(vals_rets1);
    fdb_slice_destroy(key1_14);

    //hmget
    fprintf(stdout, "hmget\n");
    fdb_slice_t* key1_15 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    fdb_array_t *flds1 = fdb_array_create(2);
    vals_rets1 = NULL;

    fdb_val_node_t *node1 = fdb_val_node_create();
    node1->val_.vval_ = fld1;

    fdb_val_node_t *node2 = fdb_val_node_create();
    node2->val_.vval_ = fld2;

    fdb_array_push_back(flds1, node1);
    fdb_array_push_back(flds1, node2);

    ret = hash_mget(ctx, slots[1], key1_15, flds1, &vals_rets1);
    assert(ret == FDB_OK);
    assert(vals_rets1->length_ == 2);
    for(int i=0; i<vals_rets1->length_; ++i){ 
        fdb_val_node_t *node = fdb_array_at(vals_rets1, i);
        fdb_slice_t *sl = (fdb_slice_t*)(node->val_.vval_);
        fprintf(stdout, "%s\n", fdb_slice_data(sl));
        fdb_slice_destroy(sl);
    }
    fdb_array_destroy(vals_rets1);
    fdb_array_destroy(flds1); 


    fdb_slice_destroy(val1);
    fdb_slice_destroy(fld1);
    fdb_slice_destroy(fld2);
    fdb_context_destroy(ctx);
    return 0;
}
