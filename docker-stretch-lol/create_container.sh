#!/bin/bash

docker run  -t  -d --privileged --name "stretch-gcc" -v /home/ariel/wmf/talks/mine/dumps-for-cpt/efficient-sort/gcc/temp:/var/tmp/build-area "ariel/gccstretch:base"


