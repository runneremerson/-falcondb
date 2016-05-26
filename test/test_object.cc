#include <falcondb/fdb_object.h>
#include <falcondb/fdb_types.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char* argv[]){
    //##test val node
    fdb_val_node_t *node0 = fdb_val_node_create();
    node0->val_.dval_=4545.2233;
    assert(node0->val_.uval_!=0);
    assert(node0->val_.vval_!=NULL);
    assert((node0->val_.dval_-4545.2233)<0.000001);
    fdb_val_node_destroy(node0);

    //##test list
    fdb_list_t *list = fdb_list_create();
    for(size_t i=0; i<10; ++i){
        fdb_val_node_t *node = fdb_val_node_create();
        node->val_.uval_ = i;
        fdb_list_push_back(list, node);
    }
    assert(list->length_ == 10);

    //iterating and push back
    fdb_list_iter_t *list_iter = fdb_list_iter_create(list);
    size_t val = 0;
    while(1){
        fdb_val_node_t *node = fdb_list_next(list_iter);
        if(node==NULL) break;
        assert(node->val_.uval_ == val);
        ++val;
    }
    assert(val==10);

    //pop front
    for(int i=0; i<5; ++i){
        fdb_val_node_t *node = fdb_list_front(list);
        assert(node->val_.uval_ == i);
        fdb_list_pop_front(list); 
        fdb_val_node_destroy(node);
    }
    assert(list->length_==5);

    //pop back
    val = 9;
    while(1){
        fdb_val_node_t *node = fdb_list_back(list);
        assert(node->val_.uval_ == val);
        fdb_list_pop_back(list);
        fdb_val_node_destroy(node);
        --val;
        if(val<5) break;
    }
    assert(list->length_==0);
    fdb_list_destroy(list);
    fdb_list_iter_destroy(list_iter);

    //push front
    list = fdb_list_create();
    for(size_t i=1; i<=10; ++i){
        fdb_val_node_t *node = fdb_val_node_create();
        node->val_.uval_ = i;
        fdb_list_push_back(list, node); 
    }
    assert(list->length_ == 10);

    val = 1;
    while(1){
        fdb_val_node_t *node = fdb_list_front(list);
        assert(node->val_.uval_==val);
        fdb_list_pop_front(list);
        fdb_val_node_destroy(node);
        ++val;
        if(val>10) break;
    }
    fdb_list_destroy(list);


    //##test array
    fdb_array_t *array = fdb_array_create(1);
    for(int i=0; i<10; ++i){
        fdb_val_node_t *node = fdb_val_node_create();
        node->val_.uval_ = i;
        fdb_array_push_back(array, node); 
    }
    
    //at
    for(int i=0; i<10; ++i){
        fdb_val_node_t *node = fdb_array_at(array, i);
        assert(node!=NULL);
        assert(node->val_.uval_ == i);
    }

    //front, back
    fdb_val_node_t *front = fdb_array_front(array);
    assert(front->val_.uval_ == 0);
    fdb_val_node_t *back = fdb_array_back(array);
    //pop back
    assert(back->val_.uval_ == 9);
    assert(fdb_array_pop_back(array) == 9);
    back = fdb_array_back(array);
    assert(back->val_.uval_ == 8);
    return 0;
}
