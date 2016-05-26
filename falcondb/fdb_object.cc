#include "fdb_types.h"
#include "fdb_object.h"
#include "fdb_malloc.h"

#include <string.h>


fdb_val_node_t* fdb_val_node_create(){
    fdb_val_node_t *node = (fdb_val_node_t*)fdb_malloc(sizeof(fdb_val_node_t));
    if(node == NULL ){
        return NULL;
    }
    memset(node, 0, sizeof(fdb_val_node_t));
    return node;
}

void fdb_val_node_destroy(fdb_val_node_t* node){
    fdb_free(node);
}

fdb_list_t* fdb_list_create(){
    fdb_list_t *list = (fdb_list_t*)fdb_malloc(sizeof(fdb_list_t));
    if(list == NULL){
        return NULL;
    }
    list->head_ = NULL;
    list->tail_ = NULL;
    list->length_ = 0;
    return list;
}

static void remove_fdb_val_node(fdb_val_node_t* node){
    if(node != NULL){
        if(node->prev_!=NULL){
            node->prev_->next_ = node->next_;
        }
        if(node->next_!=NULL){
            node->next_->prev_ = node->prev_;
        }
    }
}

void fdb_list_destroy(fdb_list_t* list){
    if(list != NULL){
        fdb_val_node_t *node=NULL;
        while(list->head_!=NULL){
            node = list->head_;
            remove_fdb_val_node(node);
            list->head_ = node->next_;
            fdb_val_node_destroy(node);
        }
        fdb_free(list);
    }
}

size_t fdb_list_push_back(fdb_list_t* list, fdb_val_node_t* node){
    if(list == NULL){
        return 0;
    }
    if(node == NULL){
        return list->length_;
    }
    if(list->tail_!=NULL){
        list->tail_->next_ = node;
        node->prev_ = list->tail_;
        list->tail_ = node;
    }else{
        list->tail_ = node;
        list->head_ = node;
    }
    list->length_ += 1;
    return list->length_;
}

size_t fdb_list_push_front(fdb_list_t* list, fdb_val_node_t* node){
    if( list == NULL ){
        return 0;
    }
    if( node == NULL ){
        return list->length_;
    }
    if( list->head_ != NULL){
        list->head_->prev_ = node;
        node->next_ = list->head_;
        list->head_ = node;
    }else{
        list->head_ = node;
        list->tail_ = node;
    }
    list->length_ += 1;
    return list->length_;
}

fdb_val_node_t* fdb_list_back(fdb_list_t* list){
    if(list!=NULL){
        return list->tail_;
    } 
    return NULL;
}


fdb_val_node_t* fdb_list_front(fdb_list_t* list){
    if(list!=NULL){
        return list->head_;
    }
    return NULL; 
}

void fdb_list_pop_front(fdb_list_t* list){
    if(list != NULL && list->length_>0){
        remove_fdb_val_node(list->head_);
        list->head_ = list->head_->next_;
        list->length_ -= 1;
        if(list->length_==0){
            list->tail_ = NULL;
        }
    }
}

void fdb_list_pop_back(fdb_list_t* list){
    if(list != NULL && list->length_>0){
        remove_fdb_val_node(list->tail_);
        list->tail_ = list->tail_->prev_;
        list->length_ -= 1;
        if(list->length_==0){
            list->head_ = NULL;
        }
    }
}

fdb_list_iter_t* fdb_list_iter_create(const fdb_list_t* list){
    if(list == NULL || list->head_== NULL){
        return NULL;
    }
    fdb_list_iter_t *iter = (fdb_list_iter_t*)fdb_malloc(sizeof(fdb_list_iter_t));
    iter->curr_ = list->head_;
    iter->length_ = list->length_;
    iter->now_ = 0;
    return iter;
}

void fdb_list_iter_destroy(fdb_list_iter_t* iter){
    fdb_free(iter);
}

fdb_val_node_t* fdb_list_next(fdb_list_iter_t* iter){
    if(iter == NULL){
        return NULL;
    }
    if(iter->curr_ == NULL){
        return NULL;
    }
    fdb_val_node_t *node = iter->curr_;
    iter->curr_ = node->next_;
    iter->now_ += 1;
    return node;
}

fdb_array_t* fdb_array_create(size_t cap){
    cap = (cap<=1)?2:cap;
    fdb_array_t *array = (fdb_array_t*)fdb_malloc(sizeof(fdb_array_t));
    array->length_ = 0;
    array->capacity_ = cap;
    array->array_ = (fdb_val_node_t**)fdb_malloc(array->capacity_ * sizeof(fdb_val_node_t*));
    return array;
}

void fdb_array_destroy(fdb_array_t* array){
    if(array!=NULL){
        fdb_free(array->array_);
    }
    fdb_free(array);
}

static size_t ensure_array_capacity(size_t len){
    size_t cap = len;
    if(cap>16){
        cap += (len>>1);
    }else{
        cap += len;
    }
    return cap;
}

size_t fdb_array_push_back(fdb_array_t* array, fdb_val_node_t* node){
    if(array->capacity_ < (array->length_ + 1)){
        array->capacity_ = ensure_array_capacity(array->length_ + 1);
        fdb_val_node_t **_array = (fdb_val_node_t**)fdb_malloc(array->capacity_ * sizeof(fdb_val_node_t*));
        for(size_t i=0; i<array->length_; ++i){
            _array[i] = array->array_[i];
        }
        fdb_free(array->array_);
        array->array_ = _array;
    }
    array->array_[array->length_] = node;
    array->length_ += 1;
    return array->length_;
}

size_t fdb_array_pop_back(fdb_array_t* array){
    if(array->length_>0){
        array->length_ -= 1;
    }
    return array->length_;
}

fdb_val_node_t* fdb_array_at(fdb_array_t* array, size_t ind){
    if(array==NULL) return NULL;
    if(array->length_<=ind) return NULL;
    return array->array_[ind];
}

fdb_val_node_t* fdb_array_back(fdb_array_t* array){
    if(array==NULL) return NULL;
    if(array->length_==0) return NULL;
    return array->array_[array->length_-1];
}

fdb_val_node_t* fdb_array_front(fdb_array_t* array){
    if(array==NULL) return NULL;
    if(array->length_==0) return NULL;
    return array->array_[0];
}

