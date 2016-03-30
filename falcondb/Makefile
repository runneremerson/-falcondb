include ../build_config.mk

FDB_OBJS = fdb_slice.o fdb_context.o fdb_malloc.o



CFLAGS+= -std=gnu99 -DUSE_TCMALLOC=1 -DUSE_INT=1 -I../ 

all: ${FDB_OBJS}
	ar -cru ../libfdb.a ${FDB_OBJS}

fdb_slice.o: fdb_slice.h fdb_slice.c
	${CC} ${CFLAGS} -c fdb_slice.c
fdb_context.o: fdb_context.h fdb_context.c
	${CC} ${CFLAGS} -c fdb_context.c
fdb_malloc.o: fdb_malloc.h fdb_malloc.c
	${CC} ${CFLAGS} -c fdb_malloc.c


clean:
	rm -f  *.o  *.a
	rm -f falcondb.test

