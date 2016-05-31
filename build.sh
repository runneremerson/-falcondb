#!/bin/sh
BASE_DIR=`pwd`
ROCKSDB_PATH="$BASE_DIR/deps/rocksdb-4.2"
SNAPPY_PATH="$BASE_DIR/deps/snappy-1.1.1"
ZLIB_PATH="$BASE_DIR/deps/zlib-1.2.8"


if test -z "$TARGET_OS"; then
	TARGET_OS=`uname -s`
fi
if test -z "$MAKE"; then
	MAKE=make
fi
if test -z "$CC"; then
	CC=gcc
fi
if test -z "$CXX"; then
	CXX=g++
fi

case "$TARGET_OS" in
    Darwin)
        #PLATFORM_CLIBS="-pthread"
		#PLATFORM_CFLAGS=""
        ;;
    Linux)
        PLATFORM_CLIBS="-pthread"
        ;;
    OS_ANDROID_CROSSCOMPILE)
        PLATFORM_CLIBS="-pthread"
        SNAPPY_HOST="--host=i386-linux"
        ;;
    CYGWIN_*)
        PLATFORM_CLIBS="-lpthread"
        ;;
    SunOS)
        PLATFORM_CLIBS="-lpthread -lrt"
        ;;
    FreeBSD)
        PLATFORM_CLIBS="-lpthread"
		MAKE=gmake
        ;;
    NetBSD)
        PLATFORM_CLIBS="-lpthread -lgcc_s"
        ;;
    OpenBSD)
        PLATFORM_CLIBS="-pthread"
        ;;
    DragonFly)
        PLATFORM_CLIBS="-lpthread"
        ;;
    HP-UX)
        PLATFORM_CLIBS="-pthread"
        ;;
    *)
        echo "Unknown platform!" >&2
        exit 1
esac


DIR=`pwd`

cd "$DIR"
cd $SNAPPY_PATH
if [ ! -f Makefile ]; then
	echo ""
	echo "##### building snappy... #####"
	#./configure  --with-pic --disable-shared --enable-static
	./configure  --disable-shared --enable-static
	# FUCK! snappy compilation doesn't work on some linux!
	find . | xargs touch
	make
	echo "##### building snappy finished #####"
	echo ""
fi

cd "$DIR"
rm -f falcondb/version.h
echo "#ifndef FALCONDB_VERSION_H" >> falcondb/version.h
echo "#define FALCONDB_VERSION_H" >> falcondb/version.h
echo "#ifndef FALCONDB_VERSION" >> falcondb/version.h
echo "#define FALCONDB_VERSION \"`cat version`\"" >> falcondb/version.h
echo "#endif" >> falcondb/version.h
echo "#endif" >> falcondb/version.h

rm -f build_config.mk
echo CC=$CC >> build_config.mk
echo CXX=$CXX >> build_config.mk
echo "MAKE=$MAKE" >> build_config.mk
echo "ROCKSDB_PATH=$ROCKSDB_PATH" >> build_config.mk
echo "SNAPPY_PATH=$SNAPPY_PATH" >> build_config.mk

echo "CXXFLAGS=" >> build_config.mk
#echo "CFLAGS = -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare" >> build_config.mk
echo "CXXFLAGS = -Wall -O0 -g -Wno-sign-compare -fpermissive -std=c++11 -DUSE_TCMALLOC -DUSE_INT" >> build_config.mk
echo "CXXFLAGS += ${PLATFORM_CFLAGS}" >> build_config.mk
echo "CXXFLAGS += -I $ROCKSDB_PATH/include" >> build_config.mk



if test -z "$TMPDIR"; then
    TMPDIR=/tmp
fi


echo "CLIBS=" >> build_config.mk
echo "CLIBS += ${PLATFORM_CLIBS} " >> build_config.mk
g++ -x c++ - -o $TMPDIR/falcondb_build_test.$$ 2>/dev/null <<EOF
	#include <gperftools/tcmalloc.h>
	int main() {}
EOF
if [ "$?" = 0 ]; then
	echo "CLIBS += -ltcmalloc" >> build_config.mk
fi

echo "CLIBS += -lrocksdb " >> build_config.mk
echo "CLIBS += -lsnappy " >> build_config.mk
echo "CLIBS += -L$ROCKSDB_PATH" >> build_config.mk
echo "CLIBS += -L$SNAPPY_PATH/.libs" >> build_config.mk


rm -f $ROCKSDB_PATH/deps.mk

echo "ROCKSDB_DEPS_CFLAGS=" >> $ROCKSDB_PATH/deps.mk
echo "ROCKSDB_DEPS_CFLAGS += -I $SNAPPY_PATH " >> $ROCKSDB_PATH/deps.mk
echo "ROCKSDB_DEPS_CFLAGS += -I $ZLIB_PATH " >> $ROCKSDB_PATH/deps.mk


echo "ROCKSDB_DEPS_LDFLAGS=" >> $ROCKSDB_PATH/deps.mk
echo "ROCKSDB_DEPS_LDFLAGS += -L $SNAPPY_PATH/.libs" >> $ROCKSDB_PATH/deps.mk
echo "ROCKSDB_DEPS_LDFLAGS += -L $ZLIB_PATH" >> $ROCKSDB_PATH/deps.mk
