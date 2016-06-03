include ../build_config.mk

LIBS = ../libfdb.a 

CXXFLAGS+=  -I../  

all: simple_example.o test_context.o test_util.o test_slice.o test_bytes.o test_object.o test_keys.o test_string.o test_hash.o
	${CXX}  -o simple_example    simple_example.o     ${CLIBS}
	${CXX}  -o test_context      test_context.o       ${LIBS} ${CLIBS}
	${CXX}  -o test_util      	 test_util.o       	  ${LIBS} ${CLIBS}
	${CXX}  -o test_slice      	 test_slice.o         ${LIBS} ${CLIBS}
	${CXX}  -o test_bytes      	 test_bytes.o         ${LIBS} ${CLIBS}
	${CXX}  -o test_object       test_object.o        ${LIBS} ${CLIBS}
	${CXX}  -o test_keys       	 test_keys.o          ${LIBS} ${CLIBS}
	${CXX}  -o test_string       test_string.o        ${LIBS} ${CLIBS}
	${CXX}  -o test_hash       	 test_hash.o          ${LIBS} ${CLIBS}



simple_example.o: simple_example.cc
	${CXX} ${CXXFLAGS} -c simple_example.cc 

test_context.o: test_context.cc
	${CXX} ${CXXFLAGS} -c test_context.cc

test_util.o: test_util.cc
	${CXX} ${CXXFLAGS} -c test_util.cc

test_slice.o: test_slice.cc
	${CXX} ${CXXFLAGS} -c test_slice.cc

test_bytes.o: test_bytes.cc
	${CXX} ${CXXFLAGS} -c test_bytes.cc

test_object.o: test_object.cc
	${CXX} ${CXXFLAGS} -c test_object.cc

test_keys.o: test_keys.cc
	${CXX} ${CXXFLAGS} -c test_keys.cc

test_string.o: test_string.cc
	${CXX} ${CXXFLAGS} -c test_string.cc

test_hash.o: test_hash.cc
	${CXX} ${CXXFLAGS} -c test_hash.cc


clean:
	rm -f *.o
	rm -f simple_example
	rm -f test_context
	rm -f test_util
	rm -f test_slice
	rm -f test_bytes
	rm -f test_object
	rm -f test_string
	rm -f test_keys
	rm -f test_hash
