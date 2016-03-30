include ../build_config.mk

LIBS = ../libfdb.a 

CFLAGS+= -std=gnu99 -I../  -DUSE_TCMALLOC=1  -DUSE_INT=1 

all: simple_example.o context_test.o 
	${CXX}  -o simple_example    simple_example.o     ${CLIBS}
	${CXX}  -o context_test      context_test.o       ${LIBS} ${CLIBS}


simple_example.o: simple_example.c
	${CC} ${CFLAGS} -c simple_example.c 

context_test.o: context_test.c
	${CC} ${CFLAGS} -c context_test.c

clean:
	rm -f *.o
	rm -f simple_example
	rm -f context_test
