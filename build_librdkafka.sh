#!/bin/bash
set -o errexit
cd `dirname $0`

INSTALL_DIR=`pwd`

git submodule init
git submodule update

cd ./third_party/librdkafka
./configure --prefix=$INSTALL_DIR
make
make install
