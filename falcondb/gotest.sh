#!/usr/bin/env sh
set +e

export GOMAXPROCS=24

cd ../

BASE_DIR=`pwd`

ROCKSDB_LD_PATH=$BASE_DIR/deps/rocksdb-4.2
SNAPPY_LD_PATH=$BASE_DIR/deps/snappy-1.1.1/.libs
ROCKSDB_IN_PATH=$BASE_DIR/deps/rocksdb-4.2/include
FALCONDB_IN_PATH=$BASE_DIR/falcondb

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ROCKSDB_LD_PATH:$SNAPPY_LD_PATH

export CGO_CXXFLAGS="-g  -fpermissive -std=c++11 -I$FALCONDB_IN_PATH  -I$ROCKSDB_IN_PATH -DUSE_TCMALLOC=1 -DUSE_INT=1"
export CGO_LDFLAGS="-static-libgcc  -static-libstdc++ -L$ROCKSDB_LD_PATH -L$SNAPPY_LD_PATH -lrocksdb  -lsnappy -ltcmalloc -lm -lpthread"

cd ./falcondb

go test -c 

./falcondb.test -test.v=true

#rm -f ./falcondb.test

