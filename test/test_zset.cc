#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_context.h>
#include <falcondb/fdb_define.h>
#include <falcondb/fdb_types.h>
#include <falcondb/t_zset.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>


int main(int argc, char* argv[]){
    fdb_context_t* ctx = fdb_context_create("/tmp/falcondb_test_zset", 128, 128, 2);
    fdb_slot_t **slots = (fdb_slot_t**)ctx->slots_;

    fdb_context_drop_slot(ctx, slots[1]);
    fdb_context_create_slot(ctx, slots[1]);





    return 0;
}
