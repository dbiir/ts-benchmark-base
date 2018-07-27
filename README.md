# tsbm
ts benchmark 

# Prerequisites
1. Java >= 1.8
2. Maven >= 3.0 (If you want to compile and install IoTDB from source code)
3. TsFile >= 0.6.0 (TsFile Github page: [https://github.com/thulab/tsfile](https://github.com/thulab/tsfile))
4. IoTDB-JDBC >= 0.6.0 (IoTDB-JDBC Github page: [https://github.com/thulab/iotdb-jdbc](https://github.com/thulab/iotdb-jdbc))

# Quick Start
## Build
tsbm不需要手动编译，运行脚本会自动编译 
## Configure 
```vim conf/db.properties``` 
修改 
```DB_TYPE=iotdb``` 

```DB_IP=测试服务器IP``` 

```DB_PORT=端口号```  

```DB_USER=用户名``` 

```DB_PASSWD=用户密码``` 
### Start import
进行别的负载测试，必须先做该步骤 

```./import.sh```

可以通过console查看导入过程 

### Start write_test
```./write_test.sh``` 

测试结束后，可在 result/result.json中查看测试结果 
### Start read_test
```./read_test.sh``` 

测试结束后，可在result/result.json中查看测试结果
### Start write_test_mix.sh
```./write_test_mix.sh``` 

测试过程中 可通过log/write_test_mix_时间.log 查看日志 

测试结束后，可在result/result.json中查看测试结果 
### Start read_test_mix.sh
```./read_test_mix.sh``` 

测试过程中 可通过log/read_test_mix_时间.log 查看日志 

测试结束后，可在result/result.json中查看测试结果 
