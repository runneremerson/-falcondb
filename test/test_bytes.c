#include <falcondb/fdb_slice.h>
#include <falcondb/fdb_bytes.h>
#include <deps/rocksdb-4.2/include/rocksdb/c.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char* argv[]){
    const char* falcondb = "falcondb";
    fdb_slice_t *slice1 = fdb_slice_create(falcondb, strlen(falcondb));
    fdb_slice_uint64_push_back(slice1, -1222);
    fdb_slice_uint64_push_back(slice1, 11123);

    const char* falcondb_slice1_uint8 = "falcondb_slice1_uint8";
    fdb_slice_uint8_push_back(slice1, strlen(falcondb_slice1_uint8));
    fdb_slice_string_push_back(slice1, falcondb_slice1_uint8, strlen(falcondb_slice1_uint8));

    const char* falcondb_slice1_uint16 = "falcondb_slice1_uint16";
    fdb_slice_uint16_push_back(slice1, strlen(falcondb_slice1_uint16));
    fdb_slice_string_push_back(slice1, falcondb_slice1_uint16, strlen(falcondb_slice1_uint16));

    const char* falcondb_slice1_left = "falcondb_slice1_left";
    fdb_slice_string_push_back(slice1, falcondb_slice1_left, strlen(falcondb_slice1_left));

    fdb_bytes_t *bytes1 = fdb_bytes_create(fdb_slice_data(slice1), fdb_slice_length(slice1));

    assert(fdb_bytes_skip(bytes1, strlen(falcondb))==strlen(falcondb));
    int64_t val_int64 = 0;
    assert(fdb_bytes_read_int64(bytes1, &val_int64)==sizeof(int64_t));
    assert(val_int64 == -1222);

    uint64_t val_uint64 = 0;
    assert(fdb_bytes_read_uint64(bytes1, &val_uint64)==sizeof(uint64_t));
    assert(val_uint64 == 11123);

    fdb_slice_t *slice2 = NULL;
    assert(fdb_bytes_read_slice_len_uint8(bytes1, &slice2)==(strlen(falcondb_slice1_uint8)+sizeof(uint8_t)));
    assert(fdb_slice_length(slice2) == strlen(falcondb_slice1_uint8));
    assert(memcmp(fdb_slice_data(slice2), falcondb_slice1_uint8, strlen(falcondb_slice1_uint8))==0);

    fdb_slice_t *slice3 = NULL;
    assert(fdb_bytes_read_slice_len_uint16(bytes1, &slice3)==(strlen(falcondb_slice1_uint16)+sizeof(uint16_t)));
    assert(fdb_slice_length(slice3) == strlen(falcondb_slice1_uint16));
    assert(memcmp(fdb_slice_data(slice3), falcondb_slice1_uint16, strlen(falcondb_slice1_uint16))==0);

    fdb_slice_t *slice4 = NULL;
    assert(fdb_bytes_read_slice_len_left(bytes1, &slice4)==strlen(falcondb_slice1_left));
    assert(fdb_slice_length(slice4) == strlen(falcondb_slice1_left));
    assert(memcmp(fdb_slice_data(slice4), falcondb_slice1_left, strlen(falcondb_slice1_left))==0);



    fdb_bytes_destroy(bytes1);
    fdb_slice_destroy(slice1);
    fdb_slice_destroy(slice2);
    fdb_slice_destroy(slice3);
    fdb_slice_destroy(slice4);
    return 0;
}
