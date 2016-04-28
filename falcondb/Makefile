include ../build_config.mk

FDB_OBJS = util.o fdb_bytes.o fdb_slice.o fdb_object.o fdb_context.o fdb_malloc.o\
		   t_keys.o t_string.o t_hash.o fdb_session.o



CFLAGS+= -std=gnu99 -DUSE_TCMALLOC=1 -DUSE_INT=1 -I../ 

all: ${FDB_OBJS}
	ar -cru ../libfdb.a ${FDB_OBJS}

util.o: util.h util.c
	${CC} ${CFLAGS} -c util.c
fdb_bytes.o: fdb_bytes.h fdb_bytes.c
	${CC} ${CFLAGS} -c fdb_bytes.c
fdb_slice.o: fdb_slice.h fdb_slice.c
	${CC} ${CFLAGS} -c fdb_slice.c
fdb_object.o: fdb_object.h fdb_object.c
	${CC} ${CFLAGS} -c fdb_object.c
fdb_context.o: fdb_context.h fdb_context.c
	${CC} ${CFLAGS} -c fdb_context.c
fdb_malloc.o: fdb_malloc.h fdb_malloc.c
	${CC} ${CFLAGS} -c fdb_malloc.c
t_keys.o: t_keys.h t_keys.c
	${CC} ${CFLAGS} -c t_keys.c
t_string.o: t_string.h t_string.c
	${CC} ${CFLAGS} -c t_string.c
t_hash.o: t_hash.h t_hash.c
	${CC} ${CFLAGS} -c t_hash.c
fdb_session.o: fdb_session.h fdb_session.c
	${CC} ${CFLAGS} -c fdb_session.c


clean:
	rm -f  *.o  *.a
	rm -f falcondb.test

