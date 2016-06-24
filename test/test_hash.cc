#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/fdb_types.h>
#include <falcondb/t_hash.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>

void test_hash_get(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* sfield, const char* svalue, int retcode){
    fdb_slice_t* key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t* field = fdb_slice_create(sfield, strlen(sfield));
    fdb_slice_t* value = NULL;

    int ret = hash_get(ctx, slot, key, field, &value);
    assert(ret == retcode);
    if(ret == FDB_OK){
        if(svalue != NULL){
            assert(memcmp(fdb_slice_data(value), svalue, fdb_slice_length(value))==0);
        }
        fdb_slice_destroy(value);
    }
    fdb_slice_destroy(key);
    fdb_slice_destroy(field);
}

void test_hash_set(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* sfield, const char* svalue, int retcode, int64_t count){
    fdb_slice_t* key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t* field = fdb_slice_create(sfield, strlen(sfield));
    fdb_slice_t* value = fdb_slice_create(svalue, strlen(svalue));

    int64_t set_count = -1;
    int ret = hash_set(ctx, slot, key, field, value, &set_count);
    assert(ret == retcode);
    if(ret == FDB_OK){
        assert(count == set_count);
    }
    fdb_slice_destroy(key);
    fdb_slice_destroy(field);
    fdb_slice_destroy(value);
}

void test_hash_setnx(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* sfield, const char* svalue, int retcode, int64_t count){
    fdb_slice_t* key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t* field = fdb_slice_create(sfield, strlen(sfield));
    fdb_slice_t* value = fdb_slice_create(svalue, strlen(svalue));

    int64_t set_count = -1;
    int ret = hash_setnx(ctx, slot, key, field, value, &set_count);
    assert(ret == retcode);
    if(ret == FDB_OK){
        assert(count == set_count);
    }
    fdb_slice_destroy(key);
    fdb_slice_destroy(field);
    fdb_slice_destroy(value);
}

void test_hash_exists(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* sfield, int retcode, int64_t count){
    fdb_slice_t* key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t* field = fdb_slice_create(sfield, strlen(sfield));

    int64_t exists_count = -1;
    int ret = hash_exists(ctx, slot, key, field, &exists_count);
    assert(ret == retcode);
    assert(count==exists_count);
    fdb_slice_destroy(key);
    fdb_slice_destroy(field);
}

void test_hash_del(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* sfield, int retcode, int count){
    fdb_slice_t* key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t* field = fdb_slice_create(sfield, strlen(sfield));
    fdb_array_t* fields = fdb_array_create(2);
    fdb_val_node_t *node = fdb_val_node_create();
    node->val_.vval_ = field;
    fdb_array_push_back(fields, node);

    int64_t del_count = -1;
    int ret = hash_del(ctx, slot, key, fields, &del_count);
    assert(ret == retcode);
    assert(del_count == count);
    fdb_array_destroy(fields);
    fdb_slice_destroy(key);
    fdb_slice_destroy(field);
}

void test_hash_incr(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* sfield, int64_t init, int64_t by, int64_t val,  int retcode){
    fdb_slice_t* key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t* field = fdb_slice_create(sfield, strlen(sfield));

    int64_t incr_result = -1;
    int ret = hash_incr(ctx, slot, key, field, init, by, &incr_result);
    assert(ret == retcode);
    assert(val == incr_result);
    fdb_slice_destroy(key);
    fdb_slice_destroy(field);
}

void test_hash_length(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, int64_t length, int retcode){
    fdb_slice_t* key = fdb_slice_create(skey, strlen(skey));

    int64_t hash_length_ = -1;
    int ret = hash_length(ctx, slot, key, &hash_length_);
    assert(ret == retcode);
    if(ret == FDB_OK){
        assert(hash_length_ == length);
    }
    fdb_slice_destroy(key);
}

void print_hash_getall(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey){

}


int main(int argc, char* argv[]){

    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_hash", 128, 128, 2);
    fdb_slot_t **slots = (fdb_slot_t**)ctx->slots_;

    fdb_context_drop_slot(ctx, slots[1]);
    fdb_context_create_slot(ctx, slots[1]);

    test_hash_get(ctx, slots[1], "hash_key1", "hash_fld1", NULL, FDB_OK_NOT_EXIST);

    test_hash_set(ctx, slots[1], "hash_key1", "hash_fld1", "hash_val1", FDB_OK, 1);

    test_hash_get(ctx, slots[1], "hash_key1", "hash_fld1", "hash_val1", FDB_OK);

    test_hash_exists(ctx, slots[1], "hash_key1", "hash_fld1", FDB_OK, 1);

    test_hash_del(ctx, slots[1], "hash_key1", "hash_fld1", FDB_OK, 1);

    test_hash_get(ctx, slots[1], "hash_key1", "hash_fld1", NULL, FDB_OK_NOT_EXIST);

    test_hash_exists(ctx, slots[1], "hash_key1", "hash_fld1", FDB_OK, 0);

    test_hash_setnx(ctx, slots[1], "hash_key1", "hash_fld1", "hash_val1", FDB_OK, 1);

    test_hash_setnx(ctx, slots[1], "hash_key1", "hash_fld1", "hash_val1", FDB_OK_BUT_ALREADY_EXIST, 1);

    test_hash_incr(ctx, slots[1], "hash_key1", "hash_fld2", 0, 98, 98, FDB_OK);

    test_hash_incr(ctx, slots[1], "hash_key1", "hash_fld2", 0, -1, 97, FDB_OK);

    test_hash_length(ctx ,slots[1], "hash_key1", 2, FDB_OK);

    /*

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

    //hmset
    fprintf(stdout, "hmset\n");
    fdb_slice_t* key1_14_0 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    fdb_array_t *fldvals1 = fdb_array_create(2);

    fdb_slice_t* fld3 = fdb_slice_create("hash_fld3", strlen("hash_fld3"));
    fdb_slice_t* val3 = fdb_slice_create("hash_val3", strlen("hash_val3"));
    fdb_slice_t* fld4 = fdb_slice_create("hash_fld4", strlen("hash_fld4"));
    fdb_slice_t* val4 = fdb_slice_create("hash_val4", strlen("hash_val4"));
    fdb_val_node_t *fnode3 = fdb_val_node_create();
    fnode3->val_.vval_ = fld3;
    fdb_array_push_back(fldvals1, fnode3);

    fdb_val_node_t *vnode3 = fdb_val_node_create();
    vnode3->val_.vval_ = val3;
    fdb_array_push_back(fldvals1, vnode3);

    fdb_val_node_t *fnode4 = fdb_val_node_create();
    fnode4->val_.vval_ = fld4;
    fdb_array_push_back(fldvals1, fnode4);

    fdb_val_node_t *vnode4 = fdb_val_node_create();
    vnode4->val_.vval_ = val4;
    fdb_array_push_back(fldvals1, vnode4);

    int64_t hmset_count = -1;
    ret = hash_mset(ctx, slots[1], key1_14_0, fldvals1, &hmset_count);
    assert(ret == FDB_OK);
    assert(hmset_count == 2);

    //hmget
    fprintf(stdout, "hmget\n");
    fdb_slice_t* key1_15 = fdb_slice_create("hash_key1", strlen("hash_key1"));
    flds1 = fdb_array_create(2);
    vals_rets1 = NULL;

    fdb_val_node_t *node1 = fdb_val_node_create();
    node1->val_.vval_ = fld1;

    fdb_val_node_t *node2 = fdb_val_node_create();
    node2->val_.vval_ = fld2;

    fdb_array_push_back(flds1, node1);
    fdb_array_push_back(flds1, node2);
    fdb_array_push_back(flds1, fnode3);
    fdb_array_push_back(flds1, fnode4);

    ret = hash_mget(ctx, slots[1], key1_15, flds1, &vals_rets1);
    assert(ret == FDB_OK);
    assert(vals_rets1->length_ == 4);
    for(int i=0; i<vals_rets1->length_; ++i){ 
        fdb_val_node_t *node = fdb_array_at(vals_rets1, i);
        fdb_slice_t *sl = (fdb_slice_t*)(node->val_.vval_);
        fprintf(stdout, "%s\n", fdb_slice_data(sl));
        fdb_slice_destroy(sl);
    }
    fdb_array_destroy(vals_rets1);
    fdb_array_destroy(flds1); 


    fdb_slice_destroy(val1);
    fdb_slice_destroy(val3);
    fdb_slice_destroy(val4);
    fdb_slice_destroy(fld1);
    fdb_slice_destroy(fld2);
    fdb_slice_destroy(fld3);
    fdb_slice_destroy(fld4);
*/
    fdb_context_destroy(ctx);
    return 0;
}
