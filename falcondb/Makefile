include ../build_config.mk

FDB_OBJS = util.o fdb_bytes.o fdb_slice.o fdb_object.o fdb_context.o fdb_malloc.o fdb_iterator.o\
		   t_keys.o t_string.o t_hash.o t_zset.o t_set.o fdb_session.o



CXXFLAGS+= -I../ 

all: ${FDB_OBJS}
	ar -cru ../libfdb.a ${FDB_OBJS}

util.o: util.h util.cc
	${CXX} ${CXXFLAGS} -c util.cc
fdb_bytes.o: fdb_bytes.h fdb_bytes.cc
	${CXX} ${CXXFLAGS} -c fdb_bytes.cc
fdb_slice.o: fdb_slice.h fdb_slice.cc
	${CXX} ${CXXFLAGS} -c fdb_slice.cc
fdb_object.o: fdb_object.h fdb_object.cc
	${CXX} ${CXXFLAGS} -c fdb_object.cc
fdb_context.o: fdb_context.h fdb_context.cc
	${CXX} ${CXXFLAGS} -c fdb_context.cc
fdb_malloc.o: fdb_malloc.h fdb_malloc.cc
	${CXX} ${CXXFLAGS} -c fdb_malloc.cc
fdb_iterator.o: fdb_iterator.h fdb_iterator.cc
	${CXX} ${CXXFLAGS} -c fdb_iterator.cc
t_keys.o: t_keys.h t_keys.cc
	${CXX} ${CXXFLAGS} -c t_keys.cc
t_string.o: t_string.h t_string.cc
	${CXX} ${CXXFLAGS} -c t_string.cc
t_hash.o: t_hash.h t_hash.cc
	${CXX} ${CXXFLAGS} -c t_hash.cc
t_zset.o: t_zset.h t_zset.cc
	${CXX} ${CXXFLAGS} -c t_zset.cc
t_set.o: t_set.h t_set.cc
	${CXX} ${CXXFLAGS} -c t_set.cc
fdb_session.o: fdb_session.h fdb_session.cc
	${CXX} ${CXXFLAGS} -c fdb_session.cc


clean:
	rm -f  *.o  *.a
	rm -f falcondb.test

