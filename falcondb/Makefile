include ../build_config.mk

FDB_OBJS = util.o fdb_bytes.o fdb_slice.o fdb_context.o fdb_malloc.o t_string.o



CFLAGS+= -std=gnu99 -DUSE_TCMALLOC=1 -DUSE_INT=1 -I../ 

all: ${FDB_OBJS}
	ar -cru ../libfdb.a ${FDB_OBJS}

util.o: util.h util.c
	${CC} ${CFLAGS} -c util.c
fdb_bytes.o: fdb_bytes.h fdb_bytes.c
	${CC} ${CFLAGS} -c fdb_bytes.c
fdb_slice.o: fdb_slice.h fdb_slice.c
	${CC} ${CFLAGS} -c fdb_slice.c
fdb_context.o: fdb_context.h fdb_context.c
	${CC} ${CFLAGS} -c fdb_context.c
fdb_malloc.o: fdb_malloc.h fdb_malloc.c
	${CC} ${CFLAGS} -c fdb_malloc.c
t_string.o: t_string.h t_string.c
	${CC} ${CFLAGS} -c t_string.c


clean:
	rm -f  *.o  *.a
	rm -f falcondb.test

