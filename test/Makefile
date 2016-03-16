include ../build_config.mk

LIBS = #../libldb.a 

CFLAGS+= -DUSE_TCMALLOC=1  -DUSE_INT=1 

all: c_simple_example.o 
	${CXX}  -o c_simple_example    c_simple_example.o     ${LIBS} ${CLIBS}


c_simple_example.o: c_simple_example.c
	${CC} ${CFLAGS} -c c_simple_example.c 

clean:
	rm -rf *.o
	rm -rf c_simple_example
