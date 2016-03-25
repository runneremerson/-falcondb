include ../build_config.mk

LIBS = ../libfdb.a 

CFLAGS+= -std=gnu99 -I../  -DUSE_TCMALLOC=1  -DUSE_INT=1 

all: c_simple_example.o c_context_test.o 
	${CXX}  -o c_simple_example    c_simple_example.o     ${CLIBS}
	${CXX}  -o c_context_test      c_context_test.o       ${LIBS} ${CLIBS}


c_simple_example.o: c_simple_example.c
	${CC} ${CFLAGS} -c c_simple_example.c 

c_context_test.o: c_context_test.c
	${CC} ${CFLAGS} -c c_context_test.c

clean:
	rm -f *.o
	rm -f c_simple_example
	rm -f c_context_test
