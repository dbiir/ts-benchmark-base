# ts benchmark
this is new version of ts-benchmark,the old version please see [this](https://github.com/foruever/tsbm)   
ts benchmark is tool used to test timeseries database
# Prerequisites
1. Java >= 1.8
2. Maven >= 3.0 
# Quick Start
1. clone ts-benchmark   
` git clone git@github.com:dbiir/ts-benchmark.git`
2. build project 
` sh build.sh `
3. create new maven project and add this dependency in pom.xml
```
        <dependency>
            <groupId>cn.edu.ruc</groupId>
            <artifactId>TS-BM</artifactId>
            <version>1.0</version>
        </dependency>
```
4. implements interface `cn.edu.ruc.adapter.BaseAdapter`
5. use `TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd)`  
example:   
```
        String dataPath = "/Users/fasape/project/tsdb-test/";
        String className = "cn.edu.ruc.TimescaledbAdapter";
        String ip = "10.77.110.226";
        String port = "5432";
        String userName = "postgres";
        String passwd = "postgres";
        TSBM.startPerformTest(dataPath, className, ip, port, userName, passwd);
```

# exmaple
if you want to see detail examples please see [this project](https://github.com/foruever/tsdb-test) 


