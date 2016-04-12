#include <falcondb/fdb_slice.h>
#include <deps/rocksdb-4.2/include/rocksdb/c.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char* argv[]){

    const char* falcondb = "falcondb";
    fdb_slice_t *slice1 = fdb_slice_create(falcondb, strlen(falcondb));
    assert(fdb_slice_length(slice1) == strlen(falcondb));
    assert(memcmp(fdb_slice_data(slice1), falcondb, strlen(falcondb))==0);

    //test string push front
    const char* hello = "hello";
    fdb_slice_string_push_front(slice1, hello, strlen(hello));
    assert(fdb_slice_length(slice1)==(strlen(hello)+strlen(falcondb)));
    assert(memcmp(fdb_slice_data(slice1), hello, strlen(hello))==0);

    //test string push back
    const char* world = "world";
    fdb_slice_string_push_back(slice1, world, strlen(world));
    assert(fdb_slice_length(slice1)==(strlen(hello)+strlen(world)+strlen(falcondb)));
    assert(memcmp(fdb_slice_data(slice1)+strlen(hello)+strlen(falcondb), world, strlen(world))==0);


    //test uint8_t push front
    fdb_slice_uint8_push_front(slice1, 255);
    uint8_t val_uint8_1 = rocksdb_decode_fixed8(fdb_slice_data(slice1));
    assert(val_uint8_1==255);

    //mark the curr length
    size_t curr = fdb_slice_length(slice1);

    //test uint8_t push back
    fdb_slice_uint8_push_back(slice1, 112);
    uint8_t val_uint8_2 = rocksdb_decode_fixed8(fdb_slice_data(slice1) + curr);
    assert(val_uint8_2 == 112);

    fdb_slice_uint16_push_front(slice1, 0xFE11);
    uint16_t val_uint16_1 = rocksdb_decode_fixed16(fdb_slice_data(slice1));
    assert(val_uint16_1==0xFE11);

    curr = fdb_slice_length(slice1);

    fdb_slice_uint16_push_back(slice1, 0x11EE);
    uint16_t val_uint16_2 = rocksdb_decode_fixed16(fdb_slice_data(slice1) + curr);
    assert(val_uint16_2==0x11EE);


    //test uint32_t push front
    fdb_slice_uint32_push_front(slice1, 16800000);
    uint32_t val_uint32_1 = rocksdb_decode_fixed32(fdb_slice_data(slice1));
    assert(val_uint32_1==16800000);

    curr = fdb_slice_length(slice1);

    //test uint32_t push back
    fdb_slice_uint32_push_back(slice1, 17800000);
    uint32_t val_uint32_2 = rocksdb_decode_fixed32(fdb_slice_data(slice1)+curr);
    assert(val_uint32_2 == 17800000);

    //test uint64_t push front
    fdb_slice_uint64_push_front(slice1, 0xFFFFEFFF1111);
    uint64_t val_uint64_1 = rocksdb_decode_fixed64(fdb_slice_data(slice1));
    assert(val_uint64_1==0xFFFFEFFF1111);

    curr = fdb_slice_length(slice1);
 
    //test uint64_t push back
    fdb_slice_uint64_push_back(slice1, 0xFFFFEFFF2222);
    uint64_t val_uint64_2 = rocksdb_decode_fixed64(fdb_slice_data(slice1)+curr);
    assert(val_uint64_2==0xFFFFEFFF2222); 

    fdb_slice_destroy(slice1);
    return 0;
}
