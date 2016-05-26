$(shell sh build.sh 1>&2)
include build_config.mk



all:
	cd "${ROCKSDB_PATH}"; ${MAKE} static_lib -j 4 
	cd falcondb/; ${MAKE}
test:
	cd test/; ${MAKE}

clean:
	rm -f *.a
	cd falcondb/; ${MAKE} clean
	cd test/; ${MAKE} clean

distclean: clean
	cd "${ROCKSDB_PATH}"; ${MAKE} clean
	cd "${SNAPPY_PATH}"; ${MAKE} clean
	rm -f ${SNAPPY_PATH}/Makefile


.PHONY: all test clean distclean
