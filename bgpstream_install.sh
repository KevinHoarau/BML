#!/bin/bash
sudo apt-get update
sudo apt-get install build-essential curl zlib1g-dev libbz2-dev libcurl4-openssl-dev librdkafka-dev

mkdir src

cd src/
curl -LO https://research.wand.net.nz/software/wandio/wandio-4.2.3.tar.gz
tar zxf wandio-4.2.3.tar.gz
cd wandio-4.2.3/
./configure
make
sudo make install
sudo ldconfig
cd ../../

cd src/
curl -LO https://github.com/CAIDA/libbgpstream/releases/download/v2.2.0/libbgpstream-2.2.0.tar.gz
tar zxf libbgpstream-2.2.0.tar.gz
cd libbgpstream-2.2.0/
./configure
make
make check
sudo make install
sudo ldconfig
cd ../../
