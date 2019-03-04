# Tsbm 
if you want to test new database ,you can see [this md](https://github.com/dbiir/ts-benchmark/blob/master/doc/development.md)
timeseries db benchmark 

# Prerequisites
1. Java >= 1.8
2. Maven >= 3.0 (If you want to compile and install IoTDB from source code)

# Quick Start

## Configure 
```vim conf/db.properties``` 
to config
```DB_TYPE=iotdb``` 

```DB_IP=the database ip address``` 

```DB_PORT=the database port```  

```DB_USER=the database login username ``` 

```DB_PASSWD=the database login password ``` 
### Start import (this procedure is neccessary)
```./import.sh``` 
### Start write_test
```./write_test.sh``` 
### Start read_test
```./read_test.sh``` 
### Start write_test_mix.sh
```./write_test_mix.sh``` 
### Start read_test_mix.sh
```./read_test_mix.sh```    
the test result in document ```result/result.csv ```
