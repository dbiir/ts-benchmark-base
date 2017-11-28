package cn.edu.ruc.biz;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.lang3.StringUtils;

import cn.edu.ruc.TSUtils;
import cn.edu.ruc.TimeSlot;
import cn.edu.ruc.biz.db.BizDBUtils;
import cn.edu.ruc.biz.model.LoadRatio;
import cn.edu.ruc.biz.model.LoadRecord;
import cn.edu.ruc.biz.model.ReadRecord;
import cn.edu.ruc.biz.model.RequestThroughputRecord;
import cn.edu.ruc.biz.model.ThroughputRecord;
import cn.edu.ruc.biz.model.TimeoutRecord;
import cn.edu.ruc.biz.model.WriteRecord;
import cn.edu.ruc.db.DBBase;
import cn.edu.ruc.db.Status;
import cn.edu.ruc.db.TsPoint;
import cn.edu.ruc.enums.LoadTypeEnum;
import cn.edu.ruc.enums.ModuleEnum;
import cn.edu.ruc.enums.ReadTypeEnum;
import cn.edu.ruc.enums.StarupTypeEnum;

import com.alibaba.fastjson.JSON;

/**
 * 
 * @author sxg
 */
public class Core {
	public static final String DB_CLASS_PROERTITY="db.class";
	public static final String STARUP_TYPE_PROPERTY="starup.type";
	public static final String MODULES_PROPERTY="modules";
	public static final String LOAD_DEVICE_NUMBER_PROERTITY="load.device.number";
	public static final String LOAD_SENSOR_NUMBER_PROERTITY="load.sensor.number";
	public static final String LOAD_POINT_STEP_PROERTITY="load.point.step";
	public static final String LOAD_POINT_LOSE_RATIO_PROERTITY="load.point.lose.ratio";
	public static final String LOAD_CACHE_POINT_PROERTITY="load.cache.line";
	public static final String LOAD_IS_CONCURRENT_PROERTITY="load.is.concurrent";
	public static final String THROUGHPUT_CLIENTS_PROERTITY="throughput.clients";
	
	public static final String HISTORY_START_TIME_PROP="load.history.startTime";
	
	public static final String PERFORM_LOAD_CLIENTS_PROERTITY="perform.load.clients";
	public static final String PERFORM_LOAD_ATOM_PROERTITY="perform.load.atom.request";
	public static final String PERFORM_LOAD_EXECUTE_PROERTITY="perform.load.atom.execute.times";
	public static final String TP_CC_MAX_PROP="tp.cc.max";//throughput concurrent max
	public static final String TP_CC_BEGIN_PROP="tp.cc.begin";
	public static final String TP_CC_INTERVAL_PROP="tp.cc.interval";
	public static final String TP_CC_RUNTIMES_PROP="tp.cc.runtimes";
	
	public static final String TP_SUM_PROP="tp.sum";//throughput  normal
	public static final String TP_CLIENTS_PROP="tp.clients";
	public static final String TP_MAX_PS_PROP="tp.ps.max";
	public static final String TP_RATIO_WIRTE_PROP="tp.ratio.write";//throughput  normal
	public static final String TP_RATIO_SIMPLE_READ_PROP="tp.ratio.sread";
	public static final String TP_RATIO_ANALOSIS_READ_PROP="tp.ratio.aread";
	public static final String TP_RATIO_RANDOMWRITE_PROP="tp.ratio.rwrite";
	public static final String TP_RATIO_UPDATE_PROP="tp.ratio.update";
	
	
	
	/**线性 */
	public static final String FUNCTION_LINE_RATIO_PROERTITY="function.line.ratio";
	/**傅里叶函数*/
	public static final String FUNCTION_SIN_RATIO_PROERTITY="function.sin.ratio";
	/**方波  */
	public static final String FUNCTION_SQUARE_RATIO_PROERTITY= "funtion.square.ratio";
	/**随机数 */
	public static final String FUNCTION_RANDOM_RATIO_PROERTITY= "function.random.ratio";
	/**常数     */
	public static final String FUNCTION_CONSTANT_RATIO_PROERTITY= "function.constant.ratio";
	
	public static final int SLEEP_TIMES=5000;
	public static void main(String[] args) throws Exception {
		BizDBUtils.createTables();
		Properties props = parseArguments(args);
		Constants.STARUP_TYPE=props.getProperty(STARUP_TYPE_PROPERTY);
		if(StringUtils.isBlank(Constants.STARUP_TYPE)){
			printlnErr("-starup is necessary");
			System.exit(0);
		}
		Constants.DB_CLASS=props.getProperty(DB_CLASS_PROERTITY);
		initInnerFucntion();//初始化内置函数
		//判断启动方式
		//如果是离线生成，不建批次，直接生成数据到磁盘
		//如果是在线数据生产，建ts_load_batch批次，并将数据保存
		//如果是负载生成模式 ，判断目标数据库是否有load_batch,如果有，初始化信息，如果没有，则停止程序，并给出提示
		if(StarupTypeEnum.LOAD_OFFLINE.getValue().equals(props.getProperty(STARUP_TYPE_PROPERTY))){
			initConstant(props);
			saveLoadConstantToDB(props);
			DBBase dbBase = Constants.getDBBase();
			printlnSplitLine("start offline load generate data");
			long startTime=System.currentTimeMillis();
			long count = dbBase.generateData();
			long endTime=System.currentTimeMillis();
			printlnSplitLine("start offline load generate data");
			System.out.println("数据生成器共消耗时间["+(endTime-startTime)/1000+"]s");
			System.out.println("数据生成器共生成["+count+"]条数据");
			System.out.println("数据生成器生成速度["+(long)((float)count/(endTime-startTime)*1000)+"]points/s数据");
		}
		//稳定导入数据，方便读取的压力测试
		if(StarupTypeEnum.LOAD_ONLINE.getValue().equals(props.getProperty(STARUP_TYPE_PROPERTY))){
			initConstant(props);
			saveLoadConstantToDB(props);
			DBBase dbBase = Constants.getDBBase();
			String isCon = props.getProperty(LOAD_IS_CONCURRENT_PROERTITY,"false");
			if("true".equals(isCon)||StringUtils.isBlank(isCon)){
				loadPerformConCurrent(dbBase);
			}else{
				loadPerformSingle(dbBase);
			}
		}
		//性能测试
		//1,响应时间
		//2,同时发送n个请求，响应时间以及吞吐量
		//3,加压测试，不断增加客户端数，查看伴随着压力增大的响应时间，以及每秒钟可以处理的请求数
		if(StarupTypeEnum.PERFORM.getValue().equals(props.getProperty(STARUP_TYPE_PROPERTY))){
			Constants.initModules();
			//根据上次导入的数据，初始化参数
			if(initLoadAndPerformParam()){
				DBBase dbBase = Constants.getDBBase();
				if(Constants.MODULES.contains(ModuleEnum.TIME_OUT.getId())){
					//简单响应时间测试 单项负载
					startSimplePerform(dbBase);
				}
				
				//废弃 start，结果没啥用，后面的测试也可以替代
				if(Constants.MODULES.contains(ModuleEnum.CONCURRENT_THROUGHPUT.getId())){
					startThroughputPerform(LoadTypeEnum.MUILTI);
					startThroughputPerform(LoadTypeEnum.WRITE);
					startThroughputPerform(LoadTypeEnum.RANDOM_INSERT);
					startThroughputPerform(LoadTypeEnum.UPDATE);
					startThroughputPerform(LoadTypeEnum.SIMPLE_READ);
					startThroughputPerform(LoadTypeEnum.AGGRA_READ);
				}
				//废弃 end，结果没啥用，后面的测试也可以替代
				
				//混合负载下吞吐量测试和响应时间
				//加压模式成熟后，废弃该类型 start
				if(Constants.MODULES.contains(ModuleEnum.THROUGHPUT.getId())){
					startThroughputPerformSTV2(LoadTypeEnum.MUILTI);
				}
				//加压模式成熟后，废弃该类型 end
				
				//非append加压测试
				//每个持续60s,执行完成后休息threads s,最少10s
				//可优化
				//TODO iotDB,group by 完成后,添加group by 分析查询测试
				if(Constants.MODULES.contains(ModuleEnum.STRESS_UNAPPEND.getId())){
//					startStressUnAppend(LoadTypeEnum.AGGRA_READ);
					startStressUnAppend(LoadTypeEnum.SIMPLE_READ);
//					startStressUnAppend(LoadTypeEnum.MUILTI);
				}
			}
		}
		
		//append 加压测试
		//线程数(前置机数量)固定(k),不断增加设备数进行测试 设备数=i*k i为大于0的自然数
		//压力测试模式  多线程写入   加压写入
		if(StarupTypeEnum.SAP.getValue().equals(props.getProperty(STARUP_TYPE_PROPERTY))){
			StressAppend.main(args);
		}

		
		
		//施压模式  给西工大用
		if(StarupTypeEnum.AS2S.getValue().equals(props.getProperty(STARUP_TYPE_PROPERTY))){
			if(initLoadAndPerformParam()){
				Random r=new Random();
				int currentCount=2;
				while(true){
					try {
						ExecutorService pool = Executors.newFixedThreadPool(currentCount);
						for(int i=0;i<currentCount;i++){
							pool.execute(new Runnable() {
								@Override
								public void run() {
									startThroughputPerformSTV2(LoadTypeEnum.MUILTI);
								}
							});
						}
						pool.awaitTermination(60,TimeUnit.MINUTES);
						currentCount=r.nextInt(10)+1;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		Constants.getDBBase().cleanup();
		System.exit(0);
	}
	/**
	 * 非加载数据 压力测试
	 * 线程数(用户数)从1不断增加到200
	 * 每个用户不断的发送请求 每次持续60s,并计算平均的响应数,每次间隔3s 
	 * 线程数用线程池管理,到60s后终止线程,进行统计
	 * @notice 目前不支持混合
	 */
	private static void startStressUnAppend(LoadTypeEnum loadTypeEnum) {
		DBBase dbBase = Constants.getDBBase();
		//
		int currentClients=0;
		while(currentClients<100){
			currentClients++;
			Map<Integer,Integer> countMap=new HashMap<Integer, Integer>();
			Map<Integer,Long> timeoutMap=new HashMap<Integer, Long>();
			ExecutorService pool = Executors.newFixedThreadPool(currentClients);
			long startTime=System.currentTimeMillis();
			for( int index=0;index<currentClients;index++){
				final int thisIndex=index;
				countMap.put(thisIndex, 0);
				timeoutMap.put(thisIndex,0L);
				pool.execute(new Runnable() {
					@Override
					public void run() {
						//执行请求操作 并计数
						Integer executeType = generateExecuteTypeByLoadType(loadTypeEnum);
						long currentTime=System.currentTimeMillis();
						while(currentTime-startTime<=TimeUnit.SECONDS.toMillis(10)){
							int count=countMap.get(thisIndex);
							executeType = generateExecuteTypeByLoadType(loadTypeEnum);
							Status status;
							try {
								status = execQueryByLoadType(dbBase, executeType);
								if(status.isOK()){
									countMap.put(thisIndex,++count);
									timeoutMap.put(thisIndex, status.getCostTime()+timeoutMap.get(thisIndex));
								}else{
									printlnErr("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"); 
								}
//								Thread.sleep(10);
								currentTime=System.currentTimeMillis();
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				});
			}
			pool.shutdown();
			try {
				pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
				long currentTime=System.currentTimeMillis();
				double costTime = (currentTime-startTime)/Math.pow(1000.0, 1);
				Set<Integer> keySet = countMap.keySet();
				long sum=0;
				for(Integer key:keySet){
					sum+=countMap.get(key);
				}
				long timeoutSum=0;
				Set<Integer> keySet2 = timeoutMap.keySet();
				for(Integer key:keySet2){
					timeoutSum+=timeoutMap.get(key);
				}
				println("clients:"+currentClients+","+sum/costTime+" requests/sec,average timeout ["+(long)(timeoutSum/1000.0/sum)+" us/request]");
				Thread.sleep(TimeUnit.SECONDS.toMillis(3));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}


	/**
	 * 正常测试吞吐量
	 * 算法:
	 * 吞吐量=总请求数/(结束请求时间-开始请求时间)
	 * 响应时间=总响应时间/总成功的响应请求数
	 * @param muilti
	 */
	private static void startThroughputPerformSTV2(LoadTypeEnum loadType) {
		DBBase dbBase = Constants.getDBBase();
		Properties prop= Constants.PROPERTIES;	
		int sumCount=Integer.parseInt(prop.getProperty(TP_SUM_PROP, "2000000"));//tp.max
		int clients=Integer.parseInt(prop.getProperty(TP_CLIENTS_PROP, "100"));//tp.clients
		final int maxCountPs=Integer.parseInt(prop.getProperty(TP_MAX_PS_PROP, "100000"));//tp.max.ps
		ThroughputRecord record=new ThroughputRecord();
		ExecutorService pool = Executors.newFixedThreadPool(clients);
		int countPerThread=sumCount/clients;//每个线程的操作总数
		Long startTime=System.currentTimeMillis();
		for(int i=0;i<clients;i++){
			final int threadid=i;
			int threadOpCount=countPerThread;
			if(threadid<sumCount%clients){//分配余数部分
				threadOpCount++;
			}
			final int actualThreadOpCount=threadOpCount;
			pool.execute(new Runnable() {
				private int sumOpCount=actualThreadOpCount;
				private long opDone=0;
				private double targetOpsPerMs=maxCountPs/1000.0/clients;//一毫秒操作多少次
				private int targetOpsTickNs=(int) (1000000/targetOpsPerMs);//每隔多少纳秒操作一次
				private int currentPercent=0;
				@Override
				public void run() {
					long startTimeNanos=System.nanoTime();
					while(opDone<sumOpCount){//当没有运算结束
						//进行业务操作
						Integer executeType = generateExecuteTypeByLoadType(loadType);
						Status status;
						try {
							status = execQueryByLoadType(dbBase, executeType);
							if(status.isOK()){
								record.addSuccessTimes(executeType, status.getCostTime());
							}else{
								record.addFailedTimes(executeType);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						if(threadid==0){
							currentPercent = printProgress((int)opDone, sumOpCount, currentPercent);
						}
						opDone++;
						throttleNanos(startTimeNanos);
					}
				}
				private  void sleepUntil(long deadline) {
					while (System.nanoTime() < deadline) {
						LockSupport.parkNanos(deadline - System.nanoTime());
					}
				}
				private void throttleNanos(long startTimeNanos) {
					// throttle the operations
					if (targetOpsPerMs > 0) {
						// delay until next tick
						long deadline = startTimeNanos + opDone * targetOpsTickNs;
						sleepUntil(deadline);
					}
				}
			});
		}
		pool.shutdown();
		try {
			pool.awaitTermination(1, TimeUnit.HOURS);
			println("sumSuccessCount:"+record.getSumTimes());
			Long endTime=System.currentTimeMillis();
			println("costTime:"+((endTime-startTime)/1000.0));
			println("throughtput:"+record.getSumTimes()/((endTime-startTime)/1000.0)+" requests/sec");
			record.setCostTime((endTime-startTime)/1000.0);
			
			//timeout
			record.printlnTimeout(LoadTypeEnum.WRITE);
			record.printlnTimeout(LoadTypeEnum.UPDATE);
			record.printlnTimeout(LoadTypeEnum.SIMPLE_READ);
			record.printlnTimeout(LoadTypeEnum.AGGRA_READ);
			record.printlnTimeout(LoadTypeEnum.RANDOM_INSERT);
			BizDBUtils.insertThroughputRecord(record);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//都加压都并发20000个请求
	/**
	 * 并发测试吞吐量
	 * 算法:并发数/平均响应时间
	 * @param loadType
	 */
	private static void startThroughputPerform(final LoadTypeEnum loadType) {
		DBBase dbBase = Constants.getDBBase();
		Properties prop= Constants.PROPERTIES;
		int sumCount=Integer.parseInt(prop.getProperty(TP_CC_MAX_PROP, "20000"));//tp.cc.max
		int startCount=Integer.parseInt(prop.getProperty(TP_CC_BEGIN_PROP, "0"));//tp.cc.begin
		int currentCount=startCount;//tp.
		int interval=Integer.parseInt(prop.getProperty(TP_CC_INTERVAL_PROP, "10"));//tp.cc.interval
		int runtimes=Integer.parseInt(prop.getProperty(TP_CC_RUNTIMES_PROP, "1"));//tp.cc.runtimes
		while(currentCount<=sumCount){
			if(currentCount==0){
				currentCount+=interval;
				continue;
			}
			//发送currentCount 个请求
			for(int index=0;index<runtimes;index++){
				TimeoutRecord record=new TimeoutRecord(loadType.getId(),currentCount, currentCount);
				ExecutorService pool = Executors.newFixedThreadPool(currentCount);
				for(int i=0;i<currentCount;i++){
					pool.execute(new Runnable() {
						@Override
						public void run() {
							try {
								Integer executeType = generateExecuteTypeByLoadType(loadType);
								Status status = execQueryByLoadType(dbBase, executeType);
								if(status.isOK()){
									record.addSuccessTimes(executeType, status.getCostTime());
								}else{
									record.addFailedTimes();
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					});
				}
				pool.shutdown();
				try {
					pool.awaitTermination(1, TimeUnit.HOURS);
					record.computeTimeout();
					System.out.println(record);
					double costAVgSecond=record.getTimeoutAvg()/1000.0/1000.0;
					double throughput=record.getSuccessTimes()/costAVgSecond;
					RequestThroughputRecord tpRecord=new RequestThroughputRecord(loadType.getId(),throughput, costAVgSecond, currentCount, record.getSuccessTimes(), record.getFailedTimes());
					BizDBUtils.insertRequestTPRecord(tpRecord);
					System.out.println(tpRecord);
					Thread.sleep(100L);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			try {
				Thread.sleep(1000L);
			} catch (Exception e) {
				e.printStackTrace();
			}
			currentCount+=interval;
		}
	}

	/**
	 * 
	 * @return
	 */
	private static boolean initLoadAndPerformParam() {
		Properties prop=Constants.PROPERTIES;
		String dbClass = prop.getProperty(DB_CLASS_PROERTITY);
		String dbUrl = Constants.getDBBase().getDBUrl();
		Constants.initLoadTypeRatio();
		Map<String, Object> map = BizDBUtils.selectLoadBatchByDbAndUrl(dbClass,dbUrl);
		if(map==null){
			printlnErr("please first load data ,start up program on mode online.load mode");
			System.exit(0);
		}
		Constants.LOAD_BATCH_ID=((Number)map.get("id")).longValue();
		Constants.DEVICE_NUMBER=(int) map.get("device_num");
		Constants.SENSOR_NUMBER=(int) map.get("sensor_num");
		Constants.POINT_STEP=((Number)map.get("point_step")).longValue();
		Constants.POINT_LOSE_RATIO=(double) map.get("point_lose_ratio");
		Constants.CACHE_POINT_NUM=(int) map.get("cache_point_num");
		Constants.HISTORY_START_TIME=(long) map.get("history_start_time");
		Constants.HISTORY_END_TIME=(long) map.get("history_end_time");
		Constants.LINE_RATIO=(double) map.get("line_ratio");
		Constants.SIN_RATIO=(double) map.get("sin_ratio");
		Constants.SQUARE_RATIO=(double) map.get("square_ratio");
		Constants.RANDOM_RATIO=(double) map.get("random_ratio");
		Constants.CONSTANT_RATIO=(double) map.get("constant_ratio");
		initDeviceSensorByDB();//初始化
		String[] columns={"load_batch_id","write_ratio","simple_query_ratio","max_query_ratio","min_query_ratio",
				"avg_query_ratio","count_query_ratio","sum_query_ratio","random_insert_ratio","update_ratio","insert_perform_device_prefix"};
		Object[] values={Constants.LOAD_BATCH_ID,Constants.WRITE_RATIO,Constants.SIMPLE_QUERY_RATIO,Constants.MAX_QUERY_RATIO,Constants.MIN_QUERY_RATIO,
					Constants.AVG_QUERY_RATIO,Constants.COUNT_QUERY_RATIO,Constants.SUM_QUERY_RATIO,Constants.RANDOM_INSERT_RATIO,Constants.UPDATE_RATIO,Constants.INSERT_PERFRM_DEVICE_PREFIX};
		long performBatchId = BizDBUtils.insertBySqlAndParamAndTable(columns, values,"ts_perform_batch");
		Constants.PERFORM_BATCH_ID=performBatchId;
		return true;
	}
	private static void initDeviceSensorByDB() {
		String deviceSql="select * from ts_device_info where load_batch_id=?";
		List<Map<String, Object>> deviceList = BizDBUtils.selectListBySqlAndParam(deviceSql,Constants.LOAD_BATCH_ID);
		for(Map<String,Object> device:deviceList){ 
			long deviceId=((Number) device.get("id")).longValue();
			String deviceName=(String) device.get("name");
			Constants.DEVICE_CODES.add(deviceName);
			String sensorSql="select * from ts_sensor_info where device_id=?";
			List<Map<String, Object>> sensors = BizDBUtils.selectListBySqlAndParam(sensorSql, deviceId);
			for(Map<String,Object> sensor:sensors){
				String sensorName=(String) sensor.get("name");
				String functionType=(String) sensor.get("function_type");
				String functionId=(String) sensor.get("function_id");
				long shiftTime=((Number) sensor.get("shift_time")).longValue();
				Constants.SHIFT_TIME_MAP.put(deviceName+"_"+sensorName, shiftTime);
				if(!Constants.SENSOR_CODES.contains(sensorName)){
					Constants.SENSOR_CODES.add(sensorName);
					Constants.SENSOR_FUNCTION.put(sensorName,Constants.getFunctionByFunctionTypeAndId(functionType,functionId));
				}
			}
		}
		Random r=new Random();
		for(int j=0;j<Constants.SENSOR_NUMBER;j++){
			Long shiftTime=(long)(r.nextDouble()*Constants.SENSOR_NUMBER)*Constants.POINT_STEP;
			Constants.SHIFT_TIME_MAP.put(Constants.INSERT_PERFRM_DEVICE_PREFIX+"_"+Constants.SENSOR_CODES.get(j), shiftTime);
		}
	}
	/**
	 * 本次测试基本信息保存到数据库中
	 */
	private static void saveLoadConstantToDB(Properties properties) {
		Long currentTime=System.currentTimeMillis();
		Object[] params={Constants.DB_CLASS,currentTime,Constants.DEVICE_NUMBER,Constants.SENSOR_NUMBER,
				Constants.POINT_STEP,Constants.CACHE_POINT_NUM,Constants.POINT_LOSE_RATIO,Constants.LINE_RATIO
				,Constants.SIN_RATIO,Constants.SQUARE_RATIO,Constants.RANDOM_RATIO,Constants.CONSTANT_RATIO
				,Constants.HISTORY_START_TIME,Constants.HISTORY_END_TIME,
				1,Constants.getDBBase().getDBUrl()};
		String[] batchColumns = {"target_db","create_time","device_num","sensor_num",
							"point_step","cache_point_num","point_lose_ratio","line_ratio","sin_ratio",
							"square_ratio","random_ratio","constant_ratio","history_start_time","history_end_time",
							"data_status","db_url"};
		long batchId = BizDBUtils.insertBySqlAndParamAndTable(batchColumns, params,"ts_load_batch");
		Constants.LOAD_BATCH_ID=batchId;
		List<String> deviceCodes=Constants.DEVICE_CODES;
		Connection conn = BizDBUtils.getConnection();
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		for(String deviceCode:deviceCodes){
			System.out.println("=============开始将设备["+deviceCode+"]写入数据库=============");
			String[] columns={"create_time","name","load_batch_id"};
			Object[] diParams={currentTime,deviceCode,Constants.LOAD_BATCH_ID};
			long deviceId = BizDBUtils.insertBySqlAndParamAndTable(conn,columns, diParams, "ts_device_info");
			List<String> sensorCodes=Constants.SENSOR_CODES;
			for(String sensorCode:sensorCodes){
				Long shiftTime=getShiftTimeByDeviceAndSensor(deviceCode, sensorCode);
				FunctionParam function = getFunctionBySensor(sensorCode);
				String[] sensorColumns={"create_time","name","device_id","function_id","function_type","shift_time"};
				Object[] siParams={currentTime,sensorCode,deviceId,function.getId(),function.getFunctionType(),shiftTime};
				BizDBUtils.insertBySqlAndParamAndTable(conn,sensorColumns, siParams,"ts_sensor_info");
			}
			System.out.println("=============结束将设备["+deviceCode+"]写入数据库=============");
		}
		try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		BizDBUtils.closeConnection(conn);

	}
	public static void initConstant(Properties props) {
		String dnStr = props.getProperty(LOAD_DEVICE_NUMBER_PROERTITY,"100");
		Constants.DEVICE_NUMBER=Integer.parseInt(dnStr);
		String snStr = props.getProperty(LOAD_SENSOR_NUMBER_PROERTITY,"500");
		Constants.SENSOR_NUMBER=Integer.parseInt(snStr);
		String psStr = props.getProperty(LOAD_POINT_STEP_PROERTITY,"7000");
		Constants.POINT_STEP=Long.parseLong(psStr);
		String plrStr = props.getProperty(LOAD_POINT_LOSE_RATIO_PROERTITY,"0.01");
		Constants.POINT_LOSE_RATIO=Double.parseDouble(plrStr);
		String lcp = props.getProperty(LOAD_CACHE_POINT_PROERTITY,10000+"");
		Constants.CACHE_POINT_NUM=Integer.parseInt(lcp);
//		initInnerFucntion();//初始化内置函数
		initDeviceCodes();//初始化设备编号
		initSensorCodes();//初始化传感器编号
		initShiftTime();//初始化时间偏移量
		initSensorFunction();//初始化传感器函数
		try {//初始化历史数据开始时间，结束时间
			String historyStartTime = props.getProperty(HISTORY_START_TIME_PROP,"");
			if(StringUtils.isBlank(historyStartTime)){
				TimeSlot timeSlot = TSUtils.getRandomTimeBetween("2016-03-01 00:00:00","2017-06-30 00:00:00",TimeUnit.DAYS.toMillis(30));
				Constants.HISTORY_START_TIME=timeSlot.getStartTime();
				Constants.HISTORY_END_TIME=timeSlot.getEndTime();
			}else{
				long startTime = TSUtils.getTimeByDateStr(historyStartTime,"yyyy-MM-dd-hh:mm:ss");
				Constants.HISTORY_START_TIME=startTime;
				Constants.HISTORY_END_TIME=startTime+TimeUnit.DAYS.toMillis(30);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	/**
	 * 打印help信息
	 */
	public static void usageMessage() {
		
	}
	/**
	 * 解析客户端传过来的动态参数
	 * @param args
	 * @return
	 */
	public static Properties parseArguments(String[] args) {
	    System.err.print("Command line:");
	    for (String arg : args) {
	      System.err.print(" " + arg);
	    }
	    System.err.println();
	    Properties fileprops = new Properties();
	    int argindex = 0;

	    if (args.length == 0) {
	      usageMessage();
	      System.out.println("At least one argument specifying a db is required.");
	      System.exit(0);
	    }
	    while (args[argindex].startsWith("-")) {
	    	if (args[argindex].compareTo("-db") == 0) {
	    		argindex++;
	            if (argindex >= args.length) {
	                usageMessage();
	                System.out.println("Missing argument value for -db.");
	                System.exit(0);
	              }
	            String dbClass = args[argindex];
	            fileprops.setProperty(DB_CLASS_PROERTITY, dbClass);
	            argindex++;
	    	}else if (args[argindex].compareTo("-p") == 0) {
	            argindex++;
	            if (argindex >= args.length) {
	              usageMessage();
	              System.out.println("Missing argument value for -p");
	              System.exit(0);
	            }
	            int eq = args[argindex].indexOf('=');
	            if (eq < 0) {
	              usageMessage();
	              System.out.println("Argument '-p' expected to be in key=value format (e.g., -p operationcount=99999)");
	              System.exit(0);
	            }

	            String name = args[argindex].substring(0, eq);
	            String value = args[argindex].substring(eq + 1);
	            fileprops.put(name, value);
	            argindex++;
	          }else if(args[argindex].compareTo("-dn") == 0){//设备数
		    		argindex++;
		            if (argindex >= args.length) {
		                usageMessage();
		                System.out.println("Missing argument value for -dn.");
		                System.exit(0);
		              }
		            String value = args[argindex];
		            fileprops.setProperty(LOAD_DEVICE_NUMBER_PROERTITY, value);
		            argindex++;
	          }else if(args[argindex].compareTo("-sn") == 0){//每个设备的传感器数
	        	  argindex++;
		            if (argindex >= args.length) {
		                usageMessage();
		                System.out.println("Missing argument value for -sn.");
		                System.exit(0);
		              }
		            String value = args[argindex];
		            fileprops.setProperty(LOAD_SENSOR_NUMBER_PROERTITY, value);
		            argindex++;
	          }else if(args[argindex].compareTo("-ps") == 0){//数据收集的步长
	        	  argindex++;
		            if (argindex >= args.length) {
		                usageMessage();
		                System.out.println("Missing argument value for -ps.");
		                System.exit(0);
		              }
		            String value = args[argindex];
		            fileprops.setProperty(LOAD_POINT_STEP_PROERTITY, value);
		            argindex++;
	          }else if(args[argindex].compareTo("-plr") == 0){//数据丢失率
	        	  argindex++;
		            if (argindex >= args.length) {
		                usageMessage();
		                System.out.println("Missing argument value for -plr.");
		                System.exit(0);
		              }
		            String value = args[argindex];
		            fileprops.setProperty(LOAD_POINT_LOSE_RATIO_PROERTITY, value);
		            argindex++;
	          }else if(args[argindex].compareTo("-flr") == 0){//线性函数率
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -flr.");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(FUNCTION_LINE_RATIO_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-fsir") == 0){//傅里叶函数率
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -fsr.");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(FUNCTION_SIN_RATIO_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-fsqr") == 0){//方波率
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -fsqr.");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(FUNCTION_SQUARE_RATIO_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-frr") == 0){//随机函数率
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -frr.");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(FUNCTION_RANDOM_RATIO_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-fcr") == 0){//常数率
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -fcr.");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(FUNCTION_CONSTANT_RATIO_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-starup") == 0){//启动类型
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -starup");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(STARUP_TYPE_PROPERTY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-lcp") == 0){//缓存条数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -lcp");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(LOAD_CACHE_POINT_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-clients") == 0){//缓存条数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -clients");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(THROUGHPUT_CLIENTS_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-lat") == 0){//负载每秒钟执行请求数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -lat");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(PERFORM_LOAD_ATOM_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-lct") == 0){//负载线程数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -lct");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(PERFORM_LOAD_CLIENTS_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-let") == 0){//负载一次脉冲持剑
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -let");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(PERFORM_LOAD_EXECUTE_PROERTITY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.cc.max") == 0){//最大并发数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.cc.max");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_CC_MAX_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.cc.begin") == 0){//初始并发数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.cc.begi");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_CC_BEGIN_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.cc.interval") == 0){//并发增量
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.cc.interval");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_CC_INTERVAL_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.cc.runtimes") == 0){//每次并发执行次数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.cc.runtimes");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_CC_RUNTIMES_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-modules") == 0){//功能
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.cc.runtimes");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(MODULES_PROPERTY, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.sum") == 0){//总数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.sum");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_SUM_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.ps.max") == 0){//每秒的总数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.ps.max");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_MAX_PS_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.clients") == 0){//总客户端数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.clients");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_CLIENTS_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.ratio.write") == 0){//总客户端数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for tp.ratio.write");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_RATIO_WIRTE_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.ratio.sread") == 0){//总客户端数
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.ratio.sread");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_RATIO_SIMPLE_READ_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.ratio.aread") == 0){//分析读比例
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.ratio.aread");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_RATIO_ANALOSIS_READ_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.ratio.rwrite") == 0){//random Insert
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.ratio.rwrite");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_RATIO_RANDOMWRITE_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-tp.ratio.update") == 0){//更新比例
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -tp.ratio.update");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(TP_RATIO_UPDATE_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-import.st") == 0){//历史数据开始时间
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -import.st");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(HISTORY_START_TIME_PROP, value);
	        	  argindex++;
	          }else if(args[argindex].compareTo("-import.is.cc") == 0){//是否多线程插入数据
	        	  argindex++;
	        	  if (argindex >= args.length) {
	        		  usageMessage();
	        		  System.out.println("Missing argument value for -import.is.cc");
	        		  System.exit(0);
	        	  }
	        	  String value = args[argindex];
	        	  fileprops.setProperty(LOAD_IS_CONCURRENT_PROERTITY, value);
	        	  argindex++;
	          }else{
	            usageMessage();
	            System.out.println("Unknown option " + args[argindex]);
	            System.exit(0);
	    	}
	        if (argindex >= args.length) {
	            break;
	          }
	    }
	    Constants.PROPERTIES=fileprops;
	    return fileprops;
	}
	
	//===================================================================
	//=======================service end=================================
	//===================================================================
	/**
	 * 导入数据测试
	 * 每秒钟可以导入多少个数据点
	 * @param dbBase
	 * @throws Exception
	 */
	public static void loadPerformSingle(DBBase dbBase) throws Exception {
		int sumTimes = dbBase.getSumTimes(Constants.CACHE_POINT_NUM);
		List<TsPoint> points=new ArrayList<TsPoint>();
		long pointCountSum=0;
		long pointCostTimeSum=0;
		println("total import["+sumTimes+"]times");
		long sumSize=0;
		for(int i=0;i<sumTimes;i++){
			points = generateLoadData(sumTimes, i+1);
			LoadRecord record=new LoadRecord();
			Status status = dbBase.insertMulti(points);
			if(status.isOK()){
				long costTime = status.getCostTime();
				int count=points.size();
				double ratio=((double)count)/TimeUnit.NANOSECONDS.toMillis(costTime)*1000;
				pointCountSum+=count;
				pointCostTimeSum+=TimeUnit.NANOSECONDS.toMillis(costTime);
				record.setLoadCostTime(TimeUnit.NANOSECONDS.toMillis(costTime));
				record.setLoadPoints(count);
				record.setPps((int)ratio);
				String json = JSON.toJSONString(points);
				int size = json.length();
				double sizeRatio=size/1024.0/1024/TimeUnit.NANOSECONDS.toMillis(costTime)*1000;
				record.setLoadSize(size);
				record.setSps(sizeRatio);
				sumSize+=size;
				BizDBUtils.insertLoadRecord(record);
				System.out.println("["+(i+1)+"/"+sumTimes+"]，import["+count+"]points，["+String.format("%.2f", size/1024.0/1024)+"]MB，cost ["+TimeUnit.NANOSECONDS.toMillis(costTime) +" ms]，import speed["+(long)ratio+" points/s]["+String.format("%.2f", sizeRatio)+"MB/s]");
			}
		}
		double ratio=((double)pointCountSum*1000)/pointCostTimeSum;
		double sizeRatio=sumSize/1024.0/1024/TimeUnit.MILLISECONDS.toSeconds(pointCostTimeSum);
//		System.out.println("共导入["+sumTimes+"]次，共导入["+pointCountSum+"]个数据点，共消耗时间["+pointCostTimeSum+" ms]平均导入速度["+ratio+" points/s]");
		System.out.println("total import["+sumTimes+"]times");
		System.out.println("total import["+pointCountSum+"]points");
		System.out.println("total import["+String.format("%.2f", sumSize/1024.0/1024)+"]MB");
		System.out.println("total cost["+pointCostTimeSum+" ms]");
		System.out.println("average speed["+(long)ratio+" points/s]");
		System.out.println("average speed["+String.format("%.2f", sizeRatio)+" MB/s]");
	}
	
	private static long IMPORT_COST_TIME=0;
	/**
	 * 导入数据测试
	 * 每秒钟可以导入多少个数据点
	 * 多线程
	 * @param dbBase
	 * @throws Exception
	 */
	public static void loadPerformConCurrent(DBBase dbBase) throws Exception {
		int sumTimes = dbBase.getSumTimes(Constants.CACHE_POINT_NUM);
		long pointCountSum=0;
		long pointCostTimeSum=0;
		long costTimeSum=0L;
		println("total import["+sumTimes+"]times");
		long sumSize=0;
		for(int i=0;i<sumTimes;i++){
			Map<String, List<TsPoint>> map = generateLoadDataMap(sumTimes, i+1);
			Set<String> threads = map.keySet();
			ExecutorService pool = Executors.newFixedThreadPool(threads.size());
			LoadRecord record=new LoadRecord();
			int sumThreadPoints=0;
			int sumThreadSize=0;
			long startTime=System.nanoTime();
			for(String deviceCode:threads){
				List<TsPoint> points = map.get(deviceCode);
				sumThreadPoints+=points.size();
//				String json = JSON.toJSONString(points);//FIXME 先去掉这部分
//				sumThreadSize+=json.length();
				pool.execute(new Runnable() {
					@Override
					public void run() {
						Status status = dbBase.insertMulti(points);
						if(status.isOK()){
							synchronized (Core.class) {
								long costTime = status.getCostTime();
								IMPORT_COST_TIME+=costTime;
							}
						}
					}
				});
			}
			pool.shutdown();
			pool.awaitTermination(15, TimeUnit.MINUTES);
			long endTime=System.nanoTime();
			record.setLoadPoints(sumThreadPoints);
			record.setLoadSize(sumThreadSize);
			long avgCostTimt=IMPORT_COST_TIME/threads.size();
			record.setLoadCostTime(TimeUnit.NANOSECONDS.toMillis(avgCostTimt));
			double sizeRatio=sumThreadSize/Math.pow(1024.0, 2)*Math.pow(1000.0, 3)/avgCostTimt;
			int pointsRatio=(int)(sumThreadPoints/(avgCostTimt/Math.pow(1000.0, 3)));
			record.setPps(pointsRatio);
			record.setSps(sizeRatio);
			long costTime=endTime-startTime;
			long programRatio=(long) (sumThreadPoints/(costTime/Math.pow(1000.0, 3)));
			double avgTimeout=(double)avgCostTimt/sumThreadPoints;
//			System.out.println("["+(i+1)+"/"+sumTimes+"]，import["+sumThreadPoints+"]points，["+String.format("%.2f", sumThreadSize/1024.0/1024)+"]MB，cost ["+TimeUnit.NANOSECONDS.toMillis(avgCostTimt) +" ms]，import speed["+pointsRatio+" points/s]["+String.format("%.2f", sizeRatio)+"MB/s]");
			System.out.println("["+(i+1)+"/"+sumTimes+"]，import["+sumThreadPoints+"]points，cost ["+TimeUnit.NANOSECONDS.toMillis(costTime) +" ms]，import speed["+programRatio+" points/s],import timeout["+(long)avgTimeout+" us/kpoints]");
			sumSize+=sumThreadSize;
			pointCountSum+=sumThreadPoints;
			pointCostTimeSum+=avgCostTimt;
			costTimeSum+=costTime;
			IMPORT_COST_TIME=0;
		}
//		double ratio=(pointCountSum)/(pointCostTimeSum/Math.pow(1000.0, 3));
//		double sizeRatio=sumSize/1024.0/1024/(pointCostTimeSum/Math.pow(1000.0, 3));
		double avgTimeout=(double)pointCostTimeSum/pointCountSum;
		double programRatio=pointCountSum/(costTimeSum/Math.pow(1000.0, 3));
		System.out.println("total import["+sumTimes+"]times");
		System.out.println("total import["+pointCountSum+"]points");
//		System.out.println("total import["+String.format("%.2f", sumSize/1024.0/1024)+"]MB");
		System.out.println("total cost["+TimeUnit.NANOSECONDS.toMillis(pointCostTimeSum)+" ms]");
//		System.out.println("average speed["+(long)ratio+" points/s]");
		System.out.println("average timeout["+(long)avgTimeout+" us/kps]");
		System.out.println("average speed["+(long)programRatio+" points/s]");
//		System.out.println("average speed["+String.format("%.2f", sizeRatio)+" MB/s]");
	}
	private static void startSimplePerform(DBBase dbBase) {
		printlnSplitLine();
		printlnSplitLine("开始单项性能测试");
		printlnSplitLine("开始单项[写入]性能测试");
		singlePerform(dbBase,LoadTypeEnum.WRITE.getId());
		printlnSplitLine("结束单项[写入]性能测试");
		
		threadWait(SLEEP_TIMES);
		printlnSplitLine("开始单项[简单查询]性能测试");
		singlePerform(dbBase,LoadTypeEnum.SIMPLE_READ.getId());
		printlnSplitLine("结束单项[简单查询]性能测试");
		
		threadWait(SLEEP_TIMES);
		
		printlnSplitLine("开始单项[分析查询]性能测试");
		singlePerform(dbBase,LoadTypeEnum.AGGRA_READ.getId());
		printlnSplitLine("结束单项[分析查询]性能测试");
		threadWait(SLEEP_TIMES);
		printlnSplitLine("开始单项[更新]性能测试");
		singlePerform(dbBase,LoadTypeEnum.UPDATE.getId());
		printlnSplitLine("结束单项[更新]性能测试");
		
		printlnSplitLine("开始单项[随机插入]性能测试");
		singlePerform(dbBase,LoadTypeEnum.RANDOM_INSERT.getId());
		printlnSplitLine("结束单项[随机插入]性能测试");
		
		printlnSplitLine();
		printlnSplitLine("结束单项性能测试");
	}
	
	/**
	 * 写入性能测试
	 * 
	 * 业务流程，从一秒钟写入一个设备数据不断增加设备数，知道增加至设备增加，但是实际每秒钟插入的数据不再增加的时候，停止测试
	 * @param dbBase
	 * @deprecated 原因:实现有问题
	 */
	public static void insertPerform(DBBase dbBase){
		int deviceCount=1;
		while (!isWriteBottleNeck()){//如果没有到达写入瓶颈
			List<TsPoint> points = generateInsertData(deviceCount);
			Status status = dbBase.insertMulti(points);
			//将当前批次 结果数据存储到数据库
			//包含信息，批次号，目标写入设备数，目标每秒写入数据点数，实际每秒的实际数据点数，实际消耗时间
			if(status.isOK()){
				WriteRecord record=new WriteRecord();
				record.setTargetDnPs(deviceCount);
				record.setTargetPointPs(points.size());
				long costTime= status.getCostTime();
				int size = points.size();
				if(status.getCostTime()<=1000){
					record.setRealPointPs(size);
				}else{
					record.setRealPointPs((int)(size/(costTime/1000.0)));
				}
				System.out.println(record);
				BizDBUtils.insertWriteRecord(record);
			}
			deviceCount++;
		}
	}
	/**
	 * 某个设备设备某个传感器，一段时间内的所有值
	 * @param readType 查询类型  ReadTypeEnum
	 * @deprecated 原因:实现有问题
	 */
	@Deprecated
	public static void singleQueryPerform(final DBBase dbBase,final Integer readType){
		if(readType==null){
			return ;
		}
		int clientCount=1;
		int step=100;
		while(!isReadBottleNeck(readType)){
			ExecutorService pool = Executors.newFixedThreadPool(clientCount+1);
			ReadRecord record=new ReadRecord();
			record.setTargetTps((long)clientCount);
			record.setReadType(readType);
			for(int i=0;i<clientCount;i++){
				pool.execute(new Runnable() {
					@Override
					public void run() {
						try {
							Status status = execQueryByReadType(dbBase, readType);
							if(status.isOK()){
								record.addReadTypeTimes(readType);
								long costTime = status.getCostTime();
								if(costTime<=1000L){
									record.addRealTps();
								}
								record.addTimeOut(costTime);
							}else{
								
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
			pool.shutdown();
			while(true){
				if(pool.isTerminated()){
					record.computeTimeout();
					System.out.println(record);
					BizDBUtils.insertReadRecord(record);
					break;
				}else{
					try {
						Thread.currentThread().sleep(200L);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			try {
				//线程休息1s,释放数据库服务器性能
				Thread.currentThread().sleep(1000L);
			} catch (Exception e) {
				e.printStackTrace();
			}
//			clientCount++;
			clientCount+=step;
		}
	}
	/**
	 * 结束施加负载
	 * @param flagMap 是否启动标识，key status value:true/false
	 */
	public static void endLoad(Map<String,Boolean> flagMap){
		flagMap.put("status", false);
	}
	/**
	 * 单线程测试
	 */
	private static void singlePerform(final DBBase dbBase,final Integer loadType){
		int times=100;
		TimeoutRecord record=new TimeoutRecord(loadType, times, 1);
		int currentPercent=0;
		for(int i=0;i<times;i++){
			//每两次请求中间休息300ms
			try {
				Status status = execQueryByLoadType(dbBase, loadType);
				if(status.isOK()){
					record.addSuccessTimes(loadType, status.getCostTime());
				}else{
					record.addFailedTimes();
				}
				currentPercent = printProgress(i, times, currentPercent);
				if(loadType.equals(LoadTypeEnum.WRITE.getId())){
					Thread.sleep(1000L);
				}else{
					Thread.sleep(200L);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		record.computeTimeout();
		System.out.println(record);
		BizDBUtils.insertTimeoutRecord(record);
	}
	/**
	 * 版本三，测试响应时间
	 * @param dbBase
	 * @param loadType
	 * @return
	 * @throws Exception
	 */
	public static  Status execQueryByLoadType(DBBase dbBase,Integer loadType) throws Exception{
		Status status=null;
		TsPoint point=new TsPoint();
		Integer internal=24*60;//一天
		String deviceCode = Core.getDeviceCodeByRandom();
		String sensorCode = Core.getSensorCodeByRandom();
		point.setDeviceCode(deviceCode);
		point.setSensorCode(sensorCode);
		//FIXME 查询时间段可优化
		if(LoadTypeEnum.WRITE.getId().equals(loadType)){
			//选择当前时间 插入数据
			List<TsPoint> points = Core.generateInsertData(1);
			status = dbBase.insertMulti(points);
		}
		if(LoadTypeEnum.RANDOM_INSERT.getId().equals(loadType)){
			//随机选择15分钟   HISTORY_START_TIME之前的 
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(TSUtils.getTimeByDateStr("2016-02-15 00:00:00"),Constants.HISTORY_START_TIME,TimeUnit.SECONDS.toMillis(6));
			//生成数据6s，进行写入
			List<TsPoint> points = generateDataBetweenTime(timeSlot.getStartTime(),timeSlot.getEndTime());
			status=dbBase.insertMulti(points);
		}
		if(LoadTypeEnum.UPDATE.getId().equals(loadType)){
			//随机选择6s   -HISTORY_START_TIME 取模为0的
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME,Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(6));//时间可调整
			List<TsPoint> points = generateDataBetweenTime(timeSlot.getStartTime(),timeSlot.getEndTime());
			status=dbBase.updatePoints(points);
		}
		if(LoadTypeEnum.SIMPLE_READ.getId().equals(loadType)){
			//查一天的数据
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(360));
			status = dbBase.selectByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(LoadTypeEnum.AGGRA_READ.getId().equals(loadType)){
			//查一天的数据的最大值
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.SECONDS.toMillis(3600*24));
			status = dbBase.selectMaxByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		return status;
	}
	
	/**
	 * 生成某个时间段内的数据
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static  List<TsPoint> generateDataBetweenTime(long startTime,long endTime){
		//单线程生成
		List<TsPoint> points=new ArrayList<TsPoint>();
		int deviceSum=Constants.DEVICE_NUMBER;
		int sensorSum=Constants.SENSOR_NUMBER;
		long step=Constants.POINT_STEP;
		double loseRatio=Constants.POINT_LOSE_RATIO;
		long current=0;
		Random r=new Random();
		for(long currentTime=startTime;currentTime<=endTime;){
			for(int deviceNum=0;deviceNum<deviceSum;deviceNum++){
				String deviceCode=Constants.DEVICE_CODES.get(deviceNum);
				for(int sensorNum=0;sensorNum<sensorSum;sensorNum++){
					double randomFloat = r.nextDouble();
					if(randomFloat<(1-loseRatio)){
						TsPoint point=new TsPoint();
						point.setDeviceCode(deviceCode);
						String sensorCode = Constants.SENSOR_CODES.get(sensorNum);
						point.setSensorCode(sensorCode);
						point.setValue(Core.getValue(deviceCode,sensorCode,currentTime));
						point.setTimestamp(currentTime);
						points.add(point);
					}
					current++;
					if(current%100000==0){
						System.out.println(current);
					}
				}
			}
			currentTime+=step;
		}
		return points;
	}
	
	
	private static Status execQueryByReadType(DBBase dbBase,Integer readType) throws Exception{
		Status status=null;
		TsPoint point=new TsPoint();
		Integer internal=24*60;//一天
		String deviceCode = getDeviceCodeByRandom();
		String sensorCode = getSensorCodeByRandom();
		point.setDeviceCode(deviceCode);
		point.setSensorCode(sensorCode);
		if(ReadTypeEnum.SINGLE_READ_1.getId().equals(readType)){
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_2.getId().equals(readType)){
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			FunctionParam param = getFunctionBySensor(sensorCode);
			status = dbBase.selectByDeviceAndSensor(point,param.getMax(),param.getMin(), new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_3.getId().equals(readType)){
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_4.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectDayMaxByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_5.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectDayMinByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_6.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectDayAvgByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_7.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectHourMaxByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_8.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectHourMinByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_9.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			point.setSensorCode(null);
			status = dbBase.selectHourAvgByDevice(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_10.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMinuteMaxByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_11.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMinuteMinByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_READ_12.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMinuteAvgByDeviceAndSensor(point, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_MAX_READ.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMaxByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_MIN_READ.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectMinByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_AVG_READ.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectAvgByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_COUNT_READ.getId().equals(readType)){
			//FIXME CASSANDRA,TSFILE不支持
			TimeSlot timeSlot = TSUtils.getRandomTimeBetween(Constants.HISTORY_START_TIME, Constants.HISTORY_END_TIME,TimeUnit.MINUTES.toMillis(internal));
			status = dbBase.selectCountByDeviceAndSensor(deviceCode, sensorCode, new Date(timeSlot.getStartTime()), new Date(timeSlot.getEndTime()));
		}
		return status;
	}
	/**
	 *  @deprecated 原因:实现有问题
	 * @return
	 */
	private static boolean isWriteBottleNeck() {
		//   最新的十条插入的数据的平均值-第11条~20条插入的数据的平均值<=0       前10平均值/100.0
		long batchId=Constants.PERFORM_BATCH_ID;
		int limitCount=500;
		String countSql="select count(*) from ts_write_record where perform_batch_id="+batchId;
		int count=BizDBUtils.selectCountBySql(countSql);
		if(count<=limitCount*2){
			return false;
		}
		String avgSql1="select avg(value) from  (select real_point_ps value from ts_write_record where perform_batch_id="+batchId+" order by id desc limit 0,"+limitCount+") t";
		double avg1=BizDBUtils.selectSingleBySql(avgSql1);
		String avgSql2="select avg(value) from  (select real_point_ps value from ts_write_record where perform_batch_id="+batchId+" order by id desc limit "+limitCount+","+limitCount+") t";
		double avg2=BizDBUtils.selectSingleBySql(avgSql2);
		System.out.println("avg1="+avg1+"||avg2="+avg2);
		if(avg1-avg2<=0){
			return true;
		}else{
			return false;
		}
	}
	@Deprecated
	private static boolean isReadBottleNeck(int readType) {
		//  最新的十条每秒查询数的平均值-第11条~20条每秒查询数的平均值<=0     sn*10/100.0 
		int limitCount=500;
		long batchId=Constants.PERFORM_BATCH_ID;
		String countSql="select count(*) from ts_read_record where perform_batch_id="+batchId+" and read_type="+readType;
		int count=BizDBUtils.selectCountBySql(countSql);
		if(count<=limitCount*2){
			return false;
		}
		String avgSql1="select avg(tps) from  (select real_tps tps from ts_read_record where perform_batch_id="+batchId+" and read_type="+readType+" order by id desc limit 0,"+limitCount+") t";
		double avg1=BizDBUtils.selectSingleBySql(avgSql1);
		String avgSql2="select avg(tps) from  (select real_tps tps from ts_read_record where perform_batch_id="+batchId+" and read_type="+readType+" order by id desc limit "+limitCount+","+limitCount+") t";
		double avg2=BizDBUtils.selectSingleBySql(avgSql2);
		if(avg1-avg2<=0){
			return true;
		}else{
			return false;
		}
	}
	/**
	 * 生成实时写入数据
	 * 
	 * @return
	 */
	public static List<TsPoint> generateInsertData(Integer deviceNum){
		long currentTime=System.currentTimeMillis();
		int sensorSum=Constants.SENSOR_NUMBER;
		String deviceCode=Constants.INSERT_PERFRM_DEVICE_PREFIX;
		List<TsPoint> points=new ArrayList<TsPoint>();
		for(int i=0;i<deviceNum;i++){
			for(int j=0;j<sensorSum;j++){
				TsPoint point=new TsPoint();
				point.setDeviceCode(deviceCode+"_"+i);
				String sensorCode = Constants.SENSOR_CODES.get(j);
				point.setSensorCode(sensorCode);
				point.setValue(getValue(deviceCode,sensorCode,currentTime));
				point.setTimestamp(currentTime);
				points.add(point);
			}
		}
		return points;
	}
	
	public static List<TsPoint> generateLoadData(){
		return generateLoadData(1, 1);
	}
	/**
	 * 所有历史数据分批生成，根据当前次数和总次数生成数据
	 * 比如总共生成一个月的数据
	 * sumTimes=30,order=10
	 * 表示总共调用生成30次，现在是第10生成
	 * @param sumTimes 生成数据总调用次数
	 * @param order  当前次数 从1开始
	 * @return
	 */
	public static List<TsPoint> generateLoadData(int sumTimes,int order){
		if(sumTimes<1){
			System.out.println("生成数据时，sumTimes必须大于或者等于1");
			System.exit(0);
		}
		if(order<1){
			System.out.println("生成数据时，order必须大于或者等于1");
			System.exit(0);
		}
		if(order>sumTimes){
			System.out.println("生成数据时，sumTimes必须大于或者等于order");
			System.exit(0);
		}
		//单线程生成
		List<TsPoint> points=new ArrayList<TsPoint>();
		int deviceSum=Constants.DEVICE_NUMBER;
		int sensorSum=Constants.SENSOR_NUMBER;
		long step=Constants.POINT_STEP;
		double loseRatio=Constants.POINT_LOSE_RATIO;
		
		
		
		long startTime=(long) (Constants.HISTORY_START_TIME+
				(Constants.HISTORY_END_TIME-Constants.HISTORY_START_TIME)*((double)(order-1)/sumTimes));
		long endTime=(long) (Constants.HISTORY_START_TIME+
				(Constants.HISTORY_END_TIME-Constants.HISTORY_START_TIME)*((double)(order)/sumTimes));
		long current=0;
		Random r=new Random();
		for(long currentTime=startTime;currentTime<=endTime;){
			for(int deviceNum=0;deviceNum<deviceSum;deviceNum++){
				String deviceCode=Constants.DEVICE_CODES.get(deviceNum);
				for(int sensorNum=0;sensorNum<sensorSum;sensorNum++){
					double randomFloat = r.nextDouble();
					if(randomFloat<(1-loseRatio)){
						TsPoint point=new TsPoint();
						point.setDeviceCode(deviceCode);
						String sensorCode = Constants.SENSOR_CODES.get(sensorNum);
						point.setSensorCode(sensorCode);
						point.setValue(getValue(deviceCode,sensorCode,currentTime));
						point.setTimestamp(currentTime);
						points.add(point);
					}
					current++;
					if(current%500000==0){
						println(current+"");
					}
				}
			}
			currentTime+=step;
		}
		return points;
	}
	/**
	 * 生成数据
	 * key 设备号  value该设备号对应的数据
	 * @param sumTimes
	 * @param order
	 * @return
	 */
	public static Map<String,List<TsPoint>> generateLoadDataMap(int sumTimes,int order){
		if(sumTimes<1){
			System.out.println("生成数据时，sumTimes必须大于或者等于1");
			System.exit(0);
		}
		if(order<1){
			System.out.println("生成数据时，order必须大于或者等于1");
			System.exit(0);
		}
		if(order>sumTimes){
			System.out.println("生成数据时，sumTimes必须大于或者等于order");
			System.exit(0);
		}
		int deviceSum=Constants.DEVICE_NUMBER;
		int sensorSum=Constants.SENSOR_NUMBER;
		long step=Constants.POINT_STEP;
		double loseRatio=Constants.POINT_LOSE_RATIO;
		long startTime=(long) (Constants.HISTORY_START_TIME+
				(Constants.HISTORY_END_TIME-Constants.HISTORY_START_TIME)*((double)(order-1)/sumTimes));
		long endTime=(long) (Constants.HISTORY_START_TIME+
				(Constants.HISTORY_END_TIME-Constants.HISTORY_START_TIME)*((double)(order)/sumTimes));
		long current=0;
		Map<String,List<TsPoint>> map=new HashMap<String, List<TsPoint>>();
		for(long currentTime=startTime;currentTime<=endTime;){
			for(int deviceNum=0;deviceNum<deviceSum;deviceNum++){
				String deviceCode=Constants.DEVICE_CODES.get(deviceNum);
				List<TsPoint> points = map.get(deviceCode);
				if(points==null){
					points=new ArrayList<TsPoint>();
				}
				for(int sensorNum=0;sensorNum<sensorSum;sensorNum++){
					double randomFloat = RANDOM.nextDouble();
					if(randomFloat<(1-loseRatio)){
						TsPoint point=new TsPoint();
						point.setDeviceCode(deviceCode);
						String sensorCode = Constants.SENSOR_CODES.get(sensorNum);
						point.setSensorCode(sensorCode);
						point.setValue(getValue(deviceCode,sensorCode,currentTime));
						point.setTimestamp(currentTime);
						points.add(point);
					}
					current++;
					if(current%500000==0){
						println(current+"");
					}
				}
				
				map.put(deviceCode,points);
			}
			currentTime+=step;
		}
		return map;
	}
	/**
	 * 通过设备号和传感器号获取
	 * @param deviceCode
	 * @param sensorCode
	 * @return
	 */
	public static Object getValue(String deviceCode, String sensorCode,Long time) {
		long shirtTime=getShiftTimeByDeviceAndSensor(deviceCode,sensorCode);
		FunctionParam functionParam=getFunctionBySensor(sensorCode);
		return Function.getValueByFuntionidAndParam(functionParam.getFunctionType(), functionParam.getMax(), functionParam.getMin(), functionParam.getCycle(), time+shirtTime);
	}
	
	
	
	//===================================================================
	//=======================service end=================================
	//===================================================================
	
	
	/**
	 * 根据传感器编号获取传感器基本函数
	 * @param sensorCode
	 * @return
	 */
	public static FunctionParam getFunctionBySensor(String sensorCode) {
		return Constants.SENSOR_FUNCTION.get(sensorCode);
	}
	/**
	 * 根据设备和传感器获取时间偏移量
	 * 目的：防止同一个时间相同函数所生成的数据值一样
	 * @param deviceCode
	 * @param sensorCode
	 * @return
	 */
	private static long getShiftTimeByDeviceAndSensor(String deviceCode,
			String sensorCode) {
		Long shiftTime = Constants.SHIFT_TIME_MAP.get(deviceCode+"_"+sensorCode);
		if(shiftTime==null){
			shiftTime=0L;
		}
		return shiftTime;
	}
	/**
	 * 根据传感器数，初始化传感器编号
	 * @param sensorSum
	 * @return
	 */
	private static List<String> initSensorCodes() {
		for(int i=0;i<Constants.SENSOR_NUMBER;i++){
			String sensorCode="s_"+TSUtils.getRandomLetter(3)+"_"+i;
			Constants.SENSOR_CODES.add(sensorCode);
		}
		return Constants.SENSOR_CODES;
	}
	/**
	 * 根据设备数，初始化设备编号
	 * @param deviceSum
	 * @return
	 */
	private static List<String> initDeviceCodes() {
		for(int i=0;i<Constants.DEVICE_NUMBER;i++){
			String deviceCode="d_"+TSUtils.getRandomLetter(2)+"_"+i;
			Constants.DEVICE_CODES.add(deviceCode);
		}
		return Constants.DEVICE_CODES;
	}
	/**
	 * 初始化时间偏移量
	 */
	private static void initShiftTime() {
		long sensorSum=Constants.DEVICE_NUMBER*Constants.SENSOR_NUMBER;
		Random r=new Random();
		for(int i=0;i<Constants.DEVICE_NUMBER;i++){
			for(int j=0;j<Constants.SENSOR_NUMBER;j++){
				Long shiftTime=(long)(r.nextDouble()*sensorSum)*Constants.POINT_STEP;
				Constants.SHIFT_TIME_MAP.put(Constants.DEVICE_CODES.get(i)+"_"+Constants.SENSOR_CODES.get(j), shiftTime);
			}
		}
		for(int j=0;j<Constants.SENSOR_NUMBER;j++){
			Long shiftTime=(long)(r.nextDouble()*sensorSum)*Constants.POINT_STEP;
			Constants.SHIFT_TIME_MAP.put(Constants.INSERT_PERFRM_DEVICE_PREFIX+"_"+Constants.SENSOR_CODES.get(j), shiftTime);
		}
	}
	/**.
	 * 初始化内置函数
	 * functionParam
	 */
	public static void initInnerFucntion() {
		
		FunctionXml xml=null;
		try {
			InputStream input = Core.class.getResourceAsStream("function.xml");
			JAXBContext context = JAXBContext.newInstance(FunctionXml.class,FunctionParam.class);
			Unmarshaller unmarshaller = context.createUnmarshaller(); 
			xml = (FunctionXml)unmarshaller.unmarshal(input);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		List<FunctionParam> xmlFuctions = xml.getFunctions();
		for(FunctionParam param:xmlFuctions){
			if(param.getFunctionType().indexOf("-mono-k")!=-1){
				Constants.LINE_LIST.add(param);
			}else if(param.getFunctionType().indexOf("-mono")!=-1){
				//如果min==max则为常数，系统没有非常数的
				if(param.getMin()==param.getMax()){
					Constants.CONSTANT_LIST.add(param);
				}
			}else if(param.getFunctionType().indexOf("-sin")!=-1){
				Constants.SIN_LIST.add(param);
			}else if(param.getFunctionType().indexOf("-square")!=-1){
				Constants.SQUARE_LIST.add(param);
			}else if(param.getFunctionType().indexOf("-random")!=-1){
				Constants.RANDOM_LIST.add(param);
			}
		}
//		System.out.println("line:"+Constants.LINE_LIST.size());
//		System.out.println("sinList:"+Constants.SIN_LIST.size());
//		System.out.println("squareList:"+Constants.SQUARE_LIST.size());
//		System.out.println("randomList:"+Constants.RANDOM_LIST.size());
//		System.out.println("constantList:"+Constants.CONSTANT_LIST.size());
	}
	/**
	 * 初始化传感器函数
	 * Constants.SENSOR_FUNCTION
	 */
	public static void initSensorFunction() {
		//根据传进来的各个函数比例进行配置
		double sumRatio=Constants.CONSTANT_RATIO+Constants.LINE_RATIO+Constants.RANDOM_RATIO+Constants.SIN_RATIO+Constants.SQUARE_RATIO;
		if(sumRatio!=0
			&&Constants.CONSTANT_RATIO>=0
			&&Constants.LINE_RATIO>=0
			&&Constants.RANDOM_RATIO>=0
			&&Constants.SIN_RATIO>=0
			&&Constants.SQUARE_RATIO>=0){
			double constantArea=Constants.CONSTANT_RATIO/sumRatio;
			double lineArea=constantArea+Constants.LINE_RATIO/sumRatio;
			double randomArea=lineArea+Constants.RANDOM_RATIO/sumRatio;
			double sinArea=randomArea+Constants.SIN_RATIO/sumRatio;
			double squareArea=sinArea+Constants.SQUARE_RATIO/sumRatio;
			Random r=new Random();
			for(int i=0;i<Constants.SENSOR_NUMBER;i++){
				double property = r.nextDouble();
				FunctionParam param=null;
				Random fr=new Random();
				double middle = fr.nextDouble();
				if(property>=0&&property<constantArea){//constant
					int index=(int)(middle*Constants.CONSTANT_LIST.size());
					param=Constants.CONSTANT_LIST.get(index);
				}
				if(property>=constantArea&&property<lineArea){//line
					int index=(int)(middle*Constants.LINE_LIST.size());
					param=Constants.CONSTANT_LIST.get(index);
				}
				if(property>=lineArea&&property<randomArea){//random
					int index=(int)(middle*Constants.RANDOM_LIST.size());
					param=Constants.RANDOM_LIST.get(index);
				}
				if(property>=randomArea&&property<sinArea){//sin
					int index=(int)(middle*Constants.SIN_LIST.size());
					param=Constants.SIN_LIST.get(index);
				}
				if(property>=sinArea&&property<squareArea){//square
					int index=(int)(middle*Constants.SQUARE_LIST.size());
					param=Constants.SQUARE_LIST.get(index);
				}
				if(param==null){
					System.err.println(" initSensorFunction() 初始化函数比例有问题！");
					System.exit(0);
				}
				String sensorCode = Constants.SENSOR_CODES.get(i);
				Constants.SENSOR_FUNCTION.put(sensorCode,param);
				
			}
			
		}else{
			System.err.println("function ration must >=0 and sum>0");
			System.exit(0);
		}
	}
	/**
	 * 从已有传感器名称中获取一个传感器名称 
	 */
	public static String getSensorCodeByRandom() {
		List<String> sensors = Constants.getAllSensors();
		int size = sensors.size();
		Random r=new Random();
		double random=r.nextDouble()*size;
		String sensorCode = sensors.get((int)random);
		return sensorCode;
	}
	/**
	 * 从已有设备名称列表中获取一个设备名称
	 */
	public static String getDeviceCodeByRandom() {
		List<String> devices = Constants.getAllDevices();
		int size = devices.size();
		Random r=new Random();
		double random=r.nextDouble()*size;
		String deviceCode = devices.get((int)random);
		return deviceCode;
	}
	public static void println(String str){
		System.out.println(str);
	}
	public static void printlnErr(String str){
		System.err.println(str);
	}
	public static void printlnSplitLine(String str){
		int total=80;
		if(StringUtils.isBlank(str)){
			println(generateSymbol("=",total));
		}else{
			int length = str.length();
			if(length%2==0){
				println(generateSymbol("=",(total-length)/2)+str+generateSymbol("=",(total-length)/2));
			}else{
				println(generateSymbol("=",(total-length)/2)+str+generateSymbol("=",(total-length)/2+1));
			}
		}
	}
	public static void printlnSplitLine(){
		printlnSplitLine("");
	}
	/**
	 * 线程休息
	 * @param ms
	 */
	private static void threadWait(long ms){
		try {
//			System.out.println("线程休息["+ms/1000+"]秒，释放资源");
			Thread.sleep(ms);
		} catch (Exception e) {
		}
	}
	/**
	 * 生成num个symbol
	 * @param symbol
	 * @param num
	 * @return
	 */
	private static String generateSymbol(String symbol,int num){
		StringBuilder sc=new StringBuilder();
		for(int i=0;i<num;i++){
			sc.append(symbol);
		}
		return sc.toString();
	}
	/**
	 * 打印进度
	 * @param current
	 * @param sum
	 * @param currentPercent
	 * @return
	 */
	private static int printProgress(int current,int sum,int currentPercent){
		int percent=(int)((double)current/sum*100);
		if(percent>currentPercent){
			if(currentPercent==0){
				System.out.println("");
			}else{
				System.out.print("\b\b\b");
			}
			if(currentPercent>9){
				System.out.print("\b");
			}
			System.out.print("=>"+percent+"%");
			currentPercent=percent;
			if(currentPercent==99){
				System.out.println("");
			}
		}
		return percent;
	}
	private static final Random RANDOM=new Random();
	/**
	 * 
	 * @param loadType
	 * @return
	 */
	private static Integer generateExecuteTypeByLoadType(
			final LoadTypeEnum loadType) {
		Integer executeType=0;//当前线程的操作类型
		LoadRatio loadRatio=LoadRatio.newInstanceByLoadType(loadType.getId());
		if(LoadTypeEnum.MUILTI.getId().equals(loadType.getId())){
			double rd = RANDOM.nextDouble();
			if(rd>=loadRatio.getWriteStartRatio()&&rd<loadRatio.getWriteEndRatio()){
				executeType=LoadTypeEnum.WRITE.getId();
			}
			if(rd>=loadRatio.getRandomInsertStartRatio()&&rd<loadRatio.getRandomInsertEndRatio()){
				executeType=LoadTypeEnum.RANDOM_INSERT.getId();
			}
			if(rd>=loadRatio.getUpdateStartRatio()&&rd<loadRatio.getUpdateEndRatio()){
				executeType=LoadTypeEnum.UPDATE.getId();
			}
			if(rd>=loadRatio.getSimpleQueryStartRatio()&&rd<loadRatio.getSimpleQueryEndRatio()){
				executeType=LoadTypeEnum.SIMPLE_READ.getId();
			}
			if(rd>=loadRatio.getAggrQueryStartRatio()&&rd<loadRatio.getAggrQueryEndRatio()){
				executeType=LoadTypeEnum.AGGRA_READ.getId();
			}
		}else{
			executeType=loadType.getId();
		}
		return executeType;
	}
}

