#include <falcondb/util.h>
#include <stdio.h>

int main(int argc, char* argv[]){
    double a= 1.0;
    printbuf(&a, sizeof(a));

    double b=big_endian_double(a);
    printbuf(&b, sizeof(b));
    return 0;
}
