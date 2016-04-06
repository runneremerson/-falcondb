#ifndef FDB_UTIL_H
#define FDB_UTIL_H

#include <stdint.h>
#include <stddef.h>


int compare_with_length(const void* s1, size_t l1, const void* s2, size_t l2);


uint64_t time_ms();

void printbuf(const void* ptr, size_t length);


uint16_t big_endian_uint16(uint16_t v);

uint32_t big_endian_uint32(uint32_t v);

uint64_t big_endian_uint64(uint64_t v);

double big_endian_double(double v);

uint64_t double_to_lex(double v);

double lex_to_double(uint64_t v);

#endif //FDB_UTIL_H

