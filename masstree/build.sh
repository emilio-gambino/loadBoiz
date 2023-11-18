#!/bin/bash

autoconf
./configure --disable-assertions --with-malloc=malloc
make -j16
