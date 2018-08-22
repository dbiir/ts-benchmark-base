#! /bin/bash

OSDATE=$(echo `date +%Y%m%d_%H_%M_%S`)
sh write.sh >log/write_log_${OSDATE}.out 2>&1 &
sh read_test.sh >log/read_test_mix_log_${OSDATE}.out 2>&1 &
echo "program run success ,if wanting to see runing log please see log/"