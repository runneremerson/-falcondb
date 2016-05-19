#!/usr/bin/env sh
set +e

export GOMAXPROCS=24

cd ../
BASE_DIR=`pwd`

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"$BASE_DIR/deps/rocksdb-4.2"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"$BASE_DIR/deps/snappy-1.1.1/.libs"

cd ./falcondb

go test -c 

./falcondb.test -test.v=true

rm -f ./falcondb.test

