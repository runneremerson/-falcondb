#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/fdb_types.h>
#include <falcondb/t_zset.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

void print_zset_range(fdb_context_t* ctx, fdb_slot_t* slot, fdb_slice_t* key, int reverse){
    printf("%s reverse[%d]\n", __func__, reverse);
    fdb_array_t *range = NULL;
    int ret = zset_range(ctx, slot, key, 0, -1, reverse, &range);
    assert(ret == FDB_OK);
    for(int i=0; i<range->length_; ){ 
        fdb_val_node_t *node1 = fdb_array_at(range, i++);
        fdb_slice_t *sl = (fdb_slice_t*)(node1->val_.vval_);

        fdb_val_node_t *node2 = fdb_array_at(range, i++);
        double score = node2->val_.dval_;
        fprintf(stdout, "range = %lu, member = %s, score =%20f\n", range->length_, fdb_slice_data(sl), score);
        fdb_slice_destroy(sl);
    }
    fdb_array_destroy(range);
}

void print_zset_scan(fdb_context_t* ctx, fdb_slot_t* slot, fdb_slice_t* key, double start, double end, int reverse, uint8_t type){
    printf("%s reverse[%d], type[%u]\n", __func__, reverse, type);
    fdb_array_t *scan = NULL;
    int ret = zset_scan(ctx, slot, key, start, end, reverse, type, &scan);
    assert(ret == FDB_OK);
    for(int i=0; i<scan->length_; ){
        fdb_val_node_t *node1 = fdb_array_at(scan, i++);
        fdb_slice_t *sl = (fdb_slice_t*)(node1->val_.vval_);

        fdb_val_node_t *node2 = fdb_array_at(scan, i++);
        double score = node2->val_.dval_;
        fprintf(stdout, "scan = %lu, member = %s, score =%20f\n", scan->length_, fdb_slice_data(sl), score);
        fdb_slice_destroy(sl);
    }
    fdb_array_destroy(scan); 
}


int main(int argc, char* argv[]){
    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_zset", 128, 128, 2);
    fdb_slot_t **slots = (fdb_slot_t**)ctx->slots_;

    fdb_context_drop_slot(ctx, slots[1]);
    fdb_context_create_slot(ctx, slots[1]);

    fdb_slice_t *key1_0 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber1 = fdb_slice_create("member1", strlen("member1"));
    int ret = zset_add(ctx, slots[1], key1_0, mber1, 1.0048);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_0);


    fdb_slice_t *key1_1 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber2 = fdb_slice_create("member2", strlen("member2"));
    ret = zset_add(ctx, slots[1], key1_1, mber2, 1.2233);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_1);


    fdb_slice_t *key1_2 = fdb_slice_create("key1", strlen("key1"));
    int64_t size = 0;
    ret = zset_size(ctx, slots[1], key1_2, &size);
    assert(ret == FDB_OK);
    assert(size == 2);
    fdb_slice_destroy(key1_2);


    fdb_slice_t *key1_3 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber3 = fdb_slice_create("member3", strlen("member3"));
    ret = zset_add(ctx, slots[1], key1_3, mber3, 0.22388);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_3);

    fdb_slice_t *key1_4 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber4 = fdb_slice_create("member4", strlen("member4"));
    ret = zset_add(ctx, slots[1], key1_3, mber4, 0.22388);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_4);


    fdb_slice_t *key1_5 = fdb_slice_create("key1", strlen("key1"));
    size = 0;
    ret = zset_size(ctx, slots[1], key1_5, &size);
    assert(ret == FDB_OK);
    assert(size == 4);
    fdb_slice_destroy(key1_5);

    double score2 = 0.0;
    fdb_slice_t *key1_6 = fdb_slice_create("key1", strlen("key1"));
    ret = zset_score(ctx, slots[1], key1_6, mber2, &score2);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_6);
    assert(fabs(score2-1.2233) <= 0.000001);

    double score3 = 0.0;
    fdb_slice_t *key1_7 = fdb_slice_create("key1", strlen("key1"));
    ret = zset_score(ctx, slots[1], key1_7, mber3, &score3);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_7);
    assert(fabs(score3-0.22388) <= 0.000001);

    fdb_slice_t *key1_8 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber5 = fdb_slice_create("member5", strlen("member5"));
    ret = zset_add(ctx, slots[1], key1_8, mber5, 0.39218);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_8);


    fdb_slice_t *key1_8_0 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber6 = fdb_slice_create("member6", strlen("member6"));
    ret = zset_add(ctx, slots[1], key1_8_0, mber6, -1.39218);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_8_0);


    fdb_slice_t *key1_8_1 = fdb_slice_create("key1", strlen("key1"));
    fdb_slice_t *mber7 = fdb_slice_create("member7", strlen("member7"));
    ret = zset_add(ctx, slots[1], key1_8_1, mber7, -20.99);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_8_1);


    fdb_slice_t *key1_9 = fdb_slice_create("key1", strlen("key1"));
    size = 0;
    ret = zset_size(ctx, slots[1], key1_9, &size);
    assert(ret == FDB_OK);
    assert(size == 7);
    fdb_slice_destroy(key1_9);


    fdb_slice_t *mberx = fdb_slice_create("memberx", strlen("memberx"));
    fdb_slice_t *key1_12 = fdb_slice_create("key1", strlen("key1"));
    ret = zset_rem(ctx, slots[1], key1_12, mberx);
    assert(ret == FDB_OK_NOT_EXIST);
    fdb_slice_destroy(key1_12);


    fdb_slice_t *key1_12_0 = fdb_slice_create("key1", strlen("key1"));
    ret = zset_rem(ctx, slots[1], key1_12_0, mber5);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_12_0);


    fdb_slice_t *key1_12_1 = fdb_slice_create("key1", strlen("key1"));
    size = 0;
    ret = zset_size(ctx, slots[1], key1_12_1, &size);
    assert(ret == FDB_OK);
    assert(size == 6);
    fdb_slice_destroy(key1_12_1);


    fdb_slice_t *key1_12_2 = fdb_slice_create("key1", strlen("key1"));
    int64_t rank = -1;
    ret = zset_rank(ctx, slots[1], key1_12_2, mber3, 0, &rank);
    assert(ret == FDB_OK);
    assert(rank == 2);
    fdb_slice_destroy(key1_12_2);


    fdb_slice_t *key1_12_3 = fdb_slice_create("key1", strlen("key1"));
    score3 = 0.0;
    ret = zset_incr(ctx, slots[1], key1_12_3, mber3, 0.0, -40.1, &score3);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_12_3);
    

    fdb_slice_t *key1_12_4 = fdb_slice_create("key1", strlen("key1"));
    rank = -1;
    ret = zset_rank(ctx, slots[1], key1_12_4, mber3, 0, &rank);
    assert(ret == FDB_OK);
    assert(rank == 0);
    fdb_slice_destroy(key1_12_4);


    fdb_slice_t *key1_13_1 = fdb_slice_create("key1", strlen("key1"));
    print_zset_range(ctx, slots[1], key1_13_1, 1);
    fdb_slice_destroy(key1_13_1);
    

    fdb_slice_t *key1_13_2 = fdb_slice_create("key1", strlen("key1"));
    print_zset_range(ctx, slots[1], key1_13_2, 0);
    fdb_slice_destroy(key1_13_2);

    fdb_slice_t *key1_13_3 = fdb_slice_create("key1", strlen("key1"));
    double score8 = 0.0;
    fdb_slice_t *mber8 = fdb_slice_create("member8", strlen("member8"));
    ret = zset_incr(ctx, slots[1], key1_13_3, mber8, 0.0, -40.1, &score8);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_13_3);

    fdb_slice_t *key1_13_4 = fdb_slice_create("key1", strlen("key1"));
    double score9 = 0.0;
    fdb_slice_t *mber9 = fdb_slice_create("member9", strlen("member9"));
    ret = zset_incr(ctx, slots[1], key1_13_4, mber9, 1.0, -40.1, &score9);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key1_13_4); 

    fdb_slice_t *key1_13_5 = fdb_slice_create("key1", strlen("key1"));
    print_zset_range(ctx, slots[1], key1_13_5, 0);
    fdb_slice_destroy(key1_13_5);


    fdb_slice_t *key1_13_6 = fdb_slice_create("key1", strlen("key1"));
    print_zset_scan(ctx, slots[1], key1_13_6, -100.0, 100.0, 0, 0);
    fdb_slice_destroy(key1_13_6);


    fdb_slice_t *key1_13_7 = fdb_slice_create("key1", strlen("key1"));
    print_zset_scan(ctx, slots[1], key1_13_7, 100.0, -100.0, 1, 0);
    fdb_slice_destroy(key1_13_7);


    fdb_slice_t *key1_13_8 = fdb_slice_create("key1", strlen("key1"));
    print_zset_scan(ctx, slots[1], key1_13_8, -40.1, 1.2233, 0, (0x1 | 0x2));
    fdb_slice_destroy(key1_13_8);


    fdb_slice_t *key1_13_9 = fdb_slice_create("key1", strlen("key1"));
    print_zset_scan(ctx, slots[1], key1_13_9, 1.2233, -40.1, 1, (0x1));
    fdb_slice_destroy(key1_13_9);
    


    fdb_slice_destroy(mber1);
    fdb_slice_destroy(mber2);
    fdb_slice_destroy(mber3);
    fdb_slice_destroy(mber4);
    fdb_slice_destroy(mber5);
    fdb_slice_destroy(mber6);
    fdb_slice_destroy(mber7);
    fdb_slice_destroy(mber8);
    fdb_slice_destroy(mber9);
    fdb_slice_destroy(mberx);
    fdb_context_destroy(ctx);
    return 0;
}
