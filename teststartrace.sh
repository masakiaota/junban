#!/bin/sh

OUTER=50

for o in `seq 1 $OUTER`; do
    NUM=5
    export TS_SOCKET=outtest.socket
    for i in `seq 1 $NUM`; do
        (./ts -n echo bu)&
    done
    wait
    ./ts -w
    ./ts -K
done
