$(shell sh build.sh 1>&2)
include build_config.mk



all:
	cd "${ROCKSDB_PATH}"; ${MAKE} static_lib -j 4 
	cd "${ROCKSDB_PATH}"; ${MAKE} shared_lib
	cd falcondb/; ${MAKE}
	cd test/; ${MAKE}

clean:
	rm -f *.a
	cd falcondb/; ${MAKE} clean
	cd test/; ${MAKE} clean

clean_all: clean
	cd "${ROCKSDB_PATH}"; ${MAKE} clean
	cd "${SNAPPY_PATH}"; ${MAKE} clean
	cd "${ZLIB_PATH}"; ${MAKE} clean
	rm -f ${SNAPPY_PATH}/Makefile
	rm -f ${ZLIB_PATH}/Makefile

