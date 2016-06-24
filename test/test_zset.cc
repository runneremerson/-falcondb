#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/fdb_types.h>
#include <falcondb/t_zset.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

void print_zset_range(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, int reverse){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    printf("%s reverse[%d]\n", __func__, reverse);
    fdb_array_t *range = NULL;
    int ret = zset_range(ctx, slot, key, 0, -1, reverse, &range);
    assert(ret == FDB_OK);
    for(int i=0; i<range->length_; ){ 
        fdb_val_node_t *node2 = fdb_array_at(range, i++);
        double score = node2->val_.dval_;

        fdb_val_node_t *node1 = fdb_array_at(range, i++);
        fdb_slice_t *sl = (fdb_slice_t*)(node1->val_.vval_);
        fprintf(stdout, "range = %lu, member = %s, score =%20f\n", range->length_, fdb_slice_data(sl), score);
        fdb_slice_destroy(sl);
    }
    fdb_slice_destroy(key);
    fdb_array_destroy(range);
}

void print_zset_scan(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, double start, double end, int reverse, uint8_t type){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    printf("%s reverse[%d], type[%u]\n", __func__, reverse, type);
    fdb_array_t *scan = NULL;
    int ret = zset_scan(ctx, slot, key, start, end, reverse, type, &scan);
    assert(ret == FDB_OK);
    for(int i=0; i<scan->length_; ){
        fdb_val_node_t *node2 = fdb_array_at(scan, i++);
        double score = node2->val_.dval_;

        fdb_val_node_t *node1 = fdb_array_at(scan, i++);
        fdb_slice_t *sl = (fdb_slice_t*)(node1->val_.vval_);
        fprintf(stdout, "scan = %lu, member = %s, score =%20f\n", scan->length_, fdb_slice_data(sl), score);
        fdb_slice_destroy(sl);
    }
    fdb_slice_destroy(key);
    fdb_array_destroy(scan); 
}

void test_zset_add(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* smember, double score, int retcode, int64_t count){
    fdb_array_t *array = fdb_array_create(2);
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t *mber = fdb_slice_create(smember, strlen(smember));

    fdb_val_node_t *score_node = fdb_val_node_create();
    score_node->val_.dval_ = score;

    fdb_val_node_t *member_node = fdb_val_node_create();
    member_node->val_.vval_ = mber;

    fdb_array_push_back(array, score_node);
    fdb_array_push_back(array, member_node);
    int64_t add_count = -1;
    int ret = zset_add(ctx, slot, key, array, &add_count);
    assert(ret == retcode);
    assert(add_count == count);

    fdb_array_destroy(array);
    fdb_slice_destroy(key);
    fdb_slice_destroy(mber);
}

void test_zset_size(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, int retcode, int64_t size){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    int64_t zset_size_ = 0;
    int ret = zset_size(ctx, slot, key, &zset_size_);
    assert(ret == retcode);
    assert(size == zset_size_);
    fdb_slice_destroy(key); 
}

void test_zset_score(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* smember, int retcode, double score){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t *mber = fdb_slice_create(smember, strlen(smember));
    double zset_score_ = 0.0;
    int ret = zset_score(ctx, slot, key, mber, &zset_score_);
    assert(ret == retcode);
    fdb_slice_destroy(key);
    fdb_slice_destroy(mber);
    assert(fabs(zset_score_-score) <= 0.000001);
}

void test_zset_rem(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* smember, int retcode, int64_t count){
    fdb_slice_t *mber = fdb_slice_create(smember, strlen(smember));
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    fdb_array_t *array = fdb_array_create(2);
    fdb_val_node_t *member_node = fdb_val_node_create();
    member_node->val_.vval_ = mber;
    fdb_array_push_back(array, member_node);
    int64_t del_count = -1;
    int ret = zset_rem(ctx, slot, key, array, &del_count);
    assert(ret == retcode);
    assert(del_count == count); 
    fdb_slice_destroy(key);
    fdb_slice_destroy(mber);
    fdb_array_destroy(array);
}

void test_zset_rank(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* smember, int retcode, int rank){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t *mber = fdb_slice_create(smember, strlen(smember));
    int64_t member_rank = -1;
    int ret = zset_rank(ctx, slot, key, mber, 0, &member_rank);
    assert(ret == retcode);
    assert(member_rank==rank);
    fdb_slice_destroy(key);
    fdb_slice_destroy(mber); 
}

void test_zset_incr(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, const char* smember, int retcode, double init, double by, double score){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    fdb_slice_t *mber = fdb_slice_create(smember, strlen(smember));
    double member_score = 0.0;
    int ret = zset_incr(ctx, slot, key, mber, init, by, &member_score);
    assert(ret == FDB_OK);
    fdb_slice_destroy(key);
    fdb_slice_destroy(mber);
    assert(fabs(member_score-score)<=0.000001);
}

void test_zset_count(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, int retcode, double start, double end, uint8_t type, int64_t count){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    int64_t zset_count_ = 0;
    int ret = zset_count(ctx, slot, key, start, end, type, &zset_count_);
    assert(ret == retcode);
    fdb_slice_destroy(key);
    assert(count == zset_count_);
}

void test_zset_rem_range_by_rank(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, int retcode, int rank_start, int rank_end, int64_t count){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    int64_t rem_count = -1;
    int ret = zset_rem_range_by_rank(ctx, slot, key, 0, 1, &rem_count);
    assert(ret == FDB_OK);
    assert(count == rem_count);
    fdb_slice_destroy(key);
}

void test_zset_rem_range_by_score(fdb_context_t* ctx, fdb_slot_t* slot, const char* skey, int retcode, double sstart, double send, uint8_t type, int64_t count){
    fdb_slice_t *key = fdb_slice_create(skey, strlen(skey));
    int64_t rem_count = -1;
    int ret = zset_rem_range_by_score(ctx, slot, key, sstart, send, type, &rem_count);
    assert(ret == retcode);
    if(ret == FDB_OK){
        assert(count == rem_count);
    }
    fdb_slice_destroy(key);
}


int main(int argc, char* argv[]){
    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_zset", 128, 128, 2);
    fdb_slot_t **slots = (fdb_slot_t**)ctx->slots_;

    fdb_context_drop_slot(ctx, slots[1]);
    fdb_context_create_slot(ctx, slots[1]);


    test_zset_add(ctx, slots[1], "key1", "member1", 1.0048, FDB_OK, 1);
    test_zset_add(ctx, slots[1], "key1", "member2", 1.2234, FDB_OK, 1);


    test_zset_size(ctx, slots[1], "key1", FDB_OK, 2);


    test_zset_add(ctx, slots[1], "key1", "member3", 7.223409, FDB_OK, 1);
    test_zset_add(ctx, slots[1], "key1", "member4", 0.2231, FDB_OK, 1);


    test_zset_size(ctx, slots[1], "key1", FDB_OK, 4);



    test_zset_score(ctx, slots[1], "key1", "member2", FDB_OK, 1.2234);
    test_zset_score(ctx, slots[1], "key1", "member3", FDB_OK, 7.223409);

    test_zset_add(ctx, slots[1], "key1", "member5", 0.39218, FDB_OK, 1);
    test_zset_add(ctx, slots[1], "key1", "member6", -1.39218, FDB_OK, 1);
    test_zset_add(ctx, slots[1], "key1", "member7", -20.99, FDB_OK, 1);


    test_zset_size(ctx, slots[1], "key1", FDB_OK, 7);

    test_zset_rem(ctx, slots[1], "key1", "memberx", FDB_OK, 0);
    test_zset_rem(ctx, slots[1], "key1", "member5", FDB_OK, 1);


    test_zset_size(ctx, slots[1], "key1", FDB_OK, 6);


    //print_zset_range(ctx, slots[1], "key1", 0);

    test_zset_rank(ctx, slots[1], "key1", "member3", FDB_OK, 5);


    test_zset_incr(ctx, slots[1], "key1", "member3", FDB_OK, 0.0, -40.1, 7.223409-40.1);

    test_zset_rank(ctx, slots[1], "key1", "member3", FDB_OK, 0);


    //print_zset_range(ctx, slots[1], "key1", 1);


    //print_zset_range(ctx, slots[1], "key1", 0);

    test_zset_add(ctx, slots[1], "key1", "member8", -1233.53, FDB_OK, 1);
    test_zset_add(ctx, slots[1], "key1", "member9", -1778.5344, FDB_OK, 1);



    //print_zset_range(ctx, slots[1], "key1", 0);

    //print_zset_scan(ctx, slots[1], "key1", -2000.0, 2000.0, 0, 0);

    test_zset_count(ctx, slots[1], "key1", FDB_OK, -1778.5344, 1000.0, 0x1, 7);
    test_zset_count(ctx, slots[1], "key1", FDB_OK, -1778.5344, -1778.5344, 0x0, 1);


    test_zset_count(ctx, slots[1], "key1", FDB_OK, -1778.5344, -1778.5344, 0x1, 0);
    test_zset_count(ctx, slots[1], "key1", FDB_OK, -1778.5344, -1778.5344, 0x2, 0);
    test_zset_count(ctx, slots[1], "key1", FDB_OK, -1778.5344, -1778.5344, 0x1|0x2, 0);


    test_zset_count(ctx, slots[1], "key1", FDB_OK, -1500.5344, 1.0048, 0x1|0x2, 5);
    test_zset_count(ctx, slots[1], "key1", FDB_OK, 100.0, 100.0, 0, 0);




    test_zset_rem_range_by_rank(ctx, slots[1], "key1", FDB_OK, 0, 1, 2);

    test_zset_size(ctx, slots[1], "key1", FDB_OK, 6);

    //print_zset_range(ctx, slots[1], "key1", 0);


    test_zset_rem_range_by_rank(ctx, slots[1], "key1", FDB_OK, -2, -1, 2);

    print_zset_range(ctx, slots[1], "key1", 0);

    //add more member for test rem_range_by_score
    test_zset_add(ctx, slots[1], "key1", "member10", -200.99, FDB_OK, 1);

    test_zset_add(ctx, slots[1], "key1", "member11", -130.99334, FDB_OK, 1);

    test_zset_add(ctx, slots[1], "key1", "member12", -110.7834, FDB_OK, 1);

    test_zset_add(ctx, slots[1], "key1", "member13", 13.44434, FDB_OK, 1);
    test_zset_add(ctx, slots[1], "key1", "member14", 73.888, FDB_OK, 1);


    test_zset_rem_range_by_score(ctx, slots[1], "key1", FDB_OK_RANGE_HAVE_NONE, 73.888, 73.888, 2, 0);


    test_zset_rem_range_by_score(ctx, slots[1], "key1", FDB_OK_RANGE_HAVE_NONE, -20.99, -20.99, 2, 0);


    print_zset_scan(ctx, slots[1], "key1", -10000.0, 20000.0, 0, 0);

    test_zset_rem_range_by_score(ctx, slots[1], "key1", FDB_OK, -2.0, 1400.0, 3, 6);

    print_zset_scan(ctx, slots[1], "key1", -10000.0, 20000.0, 0, 0);

    
    test_zset_rem_range_by_score(ctx, slots[1], "key1", FDB_OK, -200.0, 1400.0, 3, 2);


    test_zset_size(ctx, slots[1], "key1", FDB_OK, 1);


    print_zset_scan(ctx, slots[1], "key1", -10000.0, 20000.0, 0, 0);
    test_zset_rem_range_by_score(ctx, slots[1], "key1", FDB_OK, -2000.0, 1400.0, 3, 1);


    test_zset_size(ctx, slots[1], "key1", FDB_OK, 0);


    fdb_context_destroy(ctx);
    return 0;
}
