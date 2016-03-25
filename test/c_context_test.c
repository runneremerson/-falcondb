#include <falcondb/fdb_context.h>
#include <falcondb/fdb_malloc.h>
#include <stdio.h>


int main(int argc, char* argv[]){
    fdb_slot_t **slots = fdb_slots_create(5);
    const char *slot_names[] = {"cf1", "cf2", "cf3", "cf4", "cf5"};
    fdb_context_t *context = fdb_context_create("/tmp/fdbcondb", 16, 32, 5, slot_names, slots);
    fdb_slots_destroy(5, slots);
    fdb_context_destroy(context);
    return 0;
}
