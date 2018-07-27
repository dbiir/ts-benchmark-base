#! /bin/bash


OSDATE=$(echo `date +%Y%m%d_%H_%M_%S`)
sh read.sh >log/read_log_${OSDATE}.out 2>&1 &
sh write_test.sh >log/write_test_mix_log_${OSDATE}.out 2>&1 &
echo "program run success ,if wanting to see runing log please see log/"