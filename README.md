# ts-benchmark

时间序列数据库基准测试工具  
ts-benchmark是用来测试时序数据库读写性能的测试工具

### 目前支持功能



1. 批量导入历史数据
1. 混合负载的吞吐量，响应时间
1. 压力测试-多用户加压测试
1. 压力测试-多设备加压测试


### Runtime Requirements
- A Linux system 
- Java Runtime Environment 1.8
- MAVEN 
- GIT1.7.10 or later 
- 当测试对象为IotDB，需要安装IotDB的JDBC
- iotJDBC安装如 https://github.com/thulab/iotdb-jdbc

### Getting Started Simply




```
git clone https://git.oschina.net/zdyfjh2017/ts-benchmark.git
linux   
cd build
#批量导入数据
./starup.sh import tsfile  -dn 2 -sn 10 -ps 7000 -lcp 50000 -p tsfile.url=jdbc:tsfile://127.0.0.1:6667/  
windows   
mvn clean package -Dmaven.test.skip=true   
starup.bat  import tsfile  -dn 2 -sn 10 -ps 7000 -lcp 50000 -p tsfile.url=jdbc:tsfile://127.0.0.1:6667/   
 ```   
``` ./starup.sh perform tsfile -modules throughput -p  tsfile.url=jdbc:tsfile://127.0.0.1:6667/ #混合负载的吞吐量，响应时间，一共发送1000000个请求，每秒最多发送 1000000个，客户端数为1000个```   

```
#压力测试-多用户加压测试
./starup.sh perform tsfile -modules stress_unappend -p  tsfile.url=jdbc:tsfile://127.0.0.1:6667/

```
```
#压力测试-多设备加压测试
./starup.sh sap tsfile -p  tsfile.url=jdbc:tsfile://127.0.0.1:6667/

```


### 参数描述   


- 第一个参数，程序运行```import``` 数据导入,```perform``` 性能测试,```sap```  多设备加压测试
- 第二个参数，目标测试数据库 目前支持四个参数 ```tsfile```,```opentsdb```,```cassandra```,```influxdb```   
- 其余参数
  
目标数据库参数```-p key=value```  
 tsfile:   
``` -p  tsfile.url=jdbc:tsfile://127.0.0.1:6667/```  tsfiledb的jdbc url   
influxdb:   
 ```-p influxdb.url=http://127.0.0.1::8086  ``` influxdb数据库url  
  ```-p influxdb.database=ruc_test1 ```  influxdb测试数据库database名称    
opentsdb:    
  ```-p OpenTSDB.url=http://127.0.0.1::4242/  ``` opentsdb数据库url   
cassandra:   
 ```-p Cassandra.url=127.0.0.1 ```    cassandra数据库url