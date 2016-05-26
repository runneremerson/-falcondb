#ifndef FDB_OBJECT_H
#define FDB_OBJECT_H

#include <stddef.h>
#include <stdint.h>



//node
typedef struct fdb_val_node_t           fdb_val_node_t;

//list
typedef struct fdb_list_t               fdb_list_t;
typedef struct fdb_iter_t               fdb_list_iter_t;
//array
typedef struct fdb_array_t              fdb_array_t;


fdb_val_node_t* fdb_val_node_create();
void fdb_val_node_destroy(fdb_val_node_t* node);

fdb_list_t* fdb_list_create();
void fdb_list_destroy(fdb_list_t* list);

size_t fdb_list_push_back(fdb_list_t* list, fdb_val_node_t* node);
size_t fdb_list_push_front(fdb_list_t* list, fdb_val_node_t* node);

fdb_val_node_t* fdb_list_back(fdb_list_t* list);
fdb_val_node_t* fdb_list_front(fdb_list_t* list);

void fdb_list_pop_back(fdb_list_t* list);
void fdb_list_pop_front(fdb_list_t* list);

fdb_list_iter_t* fdb_list_iter_create(const fdb_list_t* list);
void fdb_list_iter_destroy(fdb_list_iter_t* iter);
fdb_val_node_t* fdb_list_next(fdb_list_iter_t* iter);

fdb_array_t* fdb_array_create(size_t cap);
void fdb_array_destroy(fdb_array_t* array);

size_t fdb_array_push_back(fdb_array_t* array, fdb_val_node_t* node);
size_t fdb_array_pop_back(fdb_array_t* array);

fdb_val_node_t* fdb_array_back(fdb_array_t* array);
fdb_val_node_t* fdb_array_front(fdb_array_t* array);
fdb_val_node_t* fdb_array_at(fdb_array_t* array, size_t ind);


#endif //FDB_OBJECT_H
