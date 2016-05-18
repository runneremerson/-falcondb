#include <falcondb/fdb_context.h>
#include <falcondb/fdb_malloc.h>
#include <stdio.h>


int main(int argc, char* argv[]){
    fdb_context_t *context = fdb_context_create("/tmp/fdbcondb", 16, 32, 5);
    fdb_context_destroy(context);
    return 0;
}
