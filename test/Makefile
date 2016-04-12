include ../build_config.mk

LIBS = ../libfdb.a 

CFLAGS+= -std=gnu99 -I../  -DUSE_TCMALLOC=1  -DUSE_INT=1 

all: simple_example.o test_context.o test_util.o test_slice.o test_bytes.o
	${CXX}  -o simple_example    simple_example.o     ${CLIBS}
	${CXX}  -o test_context      test_context.o       ${LIBS} ${CLIBS}
	${CXX}  -o test_util      	 test_util.o       	  ${LIBS} ${CLIBS}
	${CXX}  -o test_slice      	 test_slice.o         ${LIBS} ${CLIBS}
	${CXX}  -o test_bytes      	 test_bytes.o         ${LIBS} ${CLIBS}



simple_example.o: simple_example.c
	${CC} ${CFLAGS} -c simple_example.c 

test_context.o: test_context.c
	${CC} ${CFLAGS} -c test_context.c

test_util.o: test_util.c
	${CC} ${CFLAGS} -c test_util.c

test_slice.o: test_slice.c
	${CC} ${CFLAGS} -c test_slice.c

test_bytes.o: test_bytes.c
	${CC} ${CFLAGS} -c test_bytes.c

clean:
	rm -f *.o
	rm -f simple_example
	rm -f test_context
	rm -f test_util
	rm -f test_slice
	rm -f test_bytes
