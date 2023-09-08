#!/bin/bash
sudo apt update
sudo apt install build-essential curl zlib1g-dev libbz2-dev libcurl4-openssl-dev librdkafka-dev autogen autoconf libtool
#libpython`python --version | cut -c 8-10`
sudo apt-get install automake libbz2-dev liblzma-dev liblzo2-dev liblz4-dev libzstd-dev libpthread-stubs0-dev
# libpthread zlib-dev

mkdir src

cd src/
curl -LO https://github.com/LibtraceTeam/wandio/archive/refs/tags/4.2.5-1.tar.gz
tar zxf 4.2.5-1.tar.gz
cd wandio-4.2.5-1/
./bootstrap.sh
./configure
make
sudo make install
sudo ldconfig
cd ../../

cd src/
curl -LO https://github.com/CAIDA/libbgpstream/releases/download/v2.2.0/libbgpstream-2.2.0.tar.gz
tar zxf libbgpstream-2.2.0.tar.gz
cd libbgpstream-2.2.0/
# Patch for : Error: could not find pthread_yield function
# https://github.com/CAIDA/libbgpstream/issues/227
sed -i '13436 i #define _GNU_SOURCE' configure 
# End Patch
./configure
make
make check
sudo make install
sudo ldconfig
cd ../../

conda install -c conda-forge ld_impl_linux-64
pip install setuptools
cd src/
curl -LO https://github.com/CAIDA/pybgpstream/releases/download/v2.0.2/pybgpstream-2.0.2.tar.gz
tar zxf pybgpstream-2.0.2.tar.gz
cd pybgpstream-2.0.2
pip install . -v
