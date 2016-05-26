#include <falcondb/util.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <string.h>

int main(int argc, char* argv[]){
    double a1= 1.0;
    //printbuf(&a1, sizeof(double));

    double a2=big_endian_double(a1);
    unsigned char a2_char_array[sizeof(double)] = {0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    assert(memcmp(&a2, a2_char_array, sizeof(double))==0);

    uint64_t a3=double_to_lex(1.0);
    unsigned char a3_char_array[sizeof(double)] = {0xbf, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    assert(memcmp(&a3, a3_char_array, sizeof(double))==0);

    //printbuf(&a2, sizeof(double));
    //printbuf(&a3, sizeof(uint64_t));

    //converte back to a1 again
    double a4 = lex_to_double(a3);
    assert(fabs(a1-a4)<=0.000001);


    double b1= -1.0;

    double b2=big_endian_double(b1);
    unsigned char b2_char_array[sizeof(double)] = {0xbf, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    assert(memcmp(&b2, b2_char_array, sizeof(double))==0);

    uint64_t b3=double_to_lex(-1.0);
    unsigned char b3_char_array[sizeof(double)] = {0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    assert(memcmp(&b3, b3_char_array, sizeof(double))==0);

    double b4 = lex_to_double(b3);
    assert(fabs(b1-b4)<=0.000001);

    double c1= 0.0;

    double c2=big_endian_double(c1);
    unsigned char c2_char_array[sizeof(double)] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    assert(memcmp(&c2, c2_char_array, sizeof(double))==0);

    uint64_t c3=double_to_lex(0.0);
    unsigned char c3_char_array[sizeof(double)] = {0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    assert(memcmp(&c3, c3_char_array, sizeof(double))==0);

    double c4 = lex_to_double(c3);
    assert(fabs(c1-c4)<=0.000001);


    //comparison among -1.0, 0.0, 1.0
    assert(memcmp(c3_char_array, b3_char_array, sizeof(double))>0);
    assert(memcmp(a3_char_array, c3_char_array, sizeof(double))>0);
    assert(memcmp(a3_char_array, b3_char_array, sizeof(double))>0);

    return 0;
}
