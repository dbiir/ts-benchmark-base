#!/bin/bash

./starup.sh import tsfile -import.is.cc true  -dn 5 -sn 50 -ps 7000 -lcp 300000 -p tsfile.url=jdbc:tsfile://$1:$2/
