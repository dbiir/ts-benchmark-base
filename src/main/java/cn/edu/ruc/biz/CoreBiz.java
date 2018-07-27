package cn.edu.ruc.biz;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.ruc.adapter.DBAdapter;
import cn.edu.ruc.base.Status;
import cn.edu.ruc.base.TsDataSource;
import cn.edu.ruc.base.TsPackage;
import cn.edu.ruc.base.TsParamConfig;
import cn.edu.ruc.base.TsQuery;
import cn.edu.ruc.base.TsReadResult;
import cn.edu.ruc.base.TsWrite;
import cn.edu.ruc.base.TsWriteResult;
import cn.edu.ruc.utils.TSUtils;
import cn.edu.ruc.utils.TimeSlot;

/**
 * 核心业务类
 * @author fasape
 */
public class CoreBiz {
	private String devicePre="d_";
	private String sensorPre="s_";
	private TsParamConfig tsParamConfig;
	private DBAdapter dbAdapter;
	private Random random=new Random	();
	private Map<String,FunctionParam> sensorFunctionMap=new HashMap<String,FunctionParam>();
	private Map<String,Long> shiftTimeMap=new HashMap<String,Long>();
	private static final Logger LOGGER=LoggerFactory.getLogger(CoreBiz.class);
	/**
	 * 插入数据
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public  TsWriteResult insertPoints() throws Exception{
		TsWriteResult result=new TsWriteResult();
		//1 根据配置生成数据
		//2 写入数据
		int clientNum=tsParamConfig.getWriteClients();
		ExecutorService pool = Executors.newFixedThreadPool(clientNum);
		CompletionService<Status> cs = new ExecutorCompletionService<Status>(pool);
		//一轮写入数据
		long currentTime=tsParamConfig.getStartTime();
		long endTime=tsParamConfig.getEndTime();
		long programStartTime=System.currentTimeMillis();
		List<Long> timeoutList=new ArrayList<>();
		List<Integer> ppsList=new ArrayList<>();
		long sumPoints=0L;
		int count=0;
		while(currentTime<endTime) {
			//设备数，客户端数，传感器数，开始时间，结束时间，
			LinkedList<TsWrite> pkgs = generatePkg(currentTime);
			long bizStartTime=System.currentTimeMillis();
			for(int clientIndex=0;clientIndex<clientNum;clientIndex++) {
				TsWrite tsWrite = pkgs.removeFirst();
				cs.submit(new Callable<Status>() {
					@Override
					public Status call() throws Exception {
						if(tsParamConfig.getBackgroupStatus().equals(1)) {
							int sleepTime = random.nextInt(tsParamConfig.getStep().intValue())+1;
							Thread.sleep(sleepTime);
//							LOGGER.info("sleep time "+sleepTime+" ms");
						}
						Status status = execWrite(dbAdapter, tsWrite);
						if(status.isOK()) {
							return Status.OK(status.getCostTime(), tsWrite.getPointsNum());
						}else {
							return status;
						}
					}
				});
			}
			int sumNum=0;
			for(int index=0;index<clientNum;index++) {
				Status status = cs.take().get();
				if(status.isOK()) {
					sumNum+=status.getPointNum();
					timeoutList.add((long)(status.getCostTime()/status.getPointNum()));//us
				}
			}
			long bizEndTime=System.currentTimeMillis();
			long bizCost=bizEndTime-bizStartTime;
			if(tsParamConfig.getBackgroupStatus().equals(1)&&bizCost<tsParamConfig.getStep()) {//如果负载均衡运行
				bizCost=tsParamConfig.getStep();
			}
			int pps=(int) (sumNum/(bizCost/Math.pow(10.0, 3)));
			sumPoints+=sumNum;
			//记录日志
			count++;
			if(count%(10)==0) {
				result=generateWriteResult(timeoutList,ppsList);
				LOGGER.info("progerss [{}/{}],pps [{} points/s],points [{},{}],timeout(us)[max:{},min:{},95:{},50:{},mean:{}]",
						(currentTime-tsParamConfig.getStartTime())/(tsParamConfig.getStep()*tsParamConfig.getCacheTimes()),
						(tsParamConfig.getEndTime()-tsParamConfig.getStartTime())/(tsParamConfig.getStep()*tsParamConfig.getCacheTimes()),
						pps,sumNum,sumPoints,result.getMaxTimeout(),result.getMinTimeout(),result.getNinty5Timeout(),result.getFiftyTimeout(),result.getMeanTimeout());
			}else {
				LOGGER.info("progerss [{}/{}],pps [{} points/s],points [{},{}]",
						(currentTime-tsParamConfig.getStartTime())/(tsParamConfig.getStep()*tsParamConfig.getCacheTimes()),
						(tsParamConfig.getEndTime()-tsParamConfig.getStartTime())/(tsParamConfig.getStep()*tsParamConfig.getCacheTimes()),
						pps,sumNum,sumPoints);

			}
			ppsList.add(pps);
			currentTime+=tsParamConfig.getStep()*tsParamConfig.getCacheTimes();
			long costTime = System.currentTimeMillis()-bizStartTime;
			if(costTime<tsParamConfig.getWritePulse()) {//每隔writePulse ms进行一批发送
				Thread.sleep(tsParamConfig.getWritePulse()-costTime);
			}
			if(tsParamConfig.getBackgroupStatus().equals(1)&&costTime<tsParamConfig.getStep()) {//如果负载均衡运行
				Thread.sleep(tsParamConfig.getStep()-costTime);
			}
		}
		pool.shutdown();
		//生成result
		result=generateWriteResult(timeoutList,ppsList);
		result.setStartTime(programStartTime);
		result.setEndTime(System.currentTimeMillis());
		result.setSumPoints(sumPoints);
		return result;
	}
	private TsWriteResult generateWriteResult(List<Long> timeoutList, List<Integer> ppsList) {
		TsWriteResult result=new TsWriteResult();
		Collections.sort(timeoutList);
		result.setMaxTimeout(timeoutList.get(timeoutList.size()-1).intValue());
		result.setMinTimeout(timeoutList.get(0).intValue());
		long sum=0;
		for(Long timeout:timeoutList) {
			sum+=timeout;
		}
		result.setMeanTimeout((int)(sum/timeoutList.size()));
		result.setFiftyTimeout(timeoutList.get((int)(timeoutList.size()*0.5)).intValue());
		result.setNinty5Timeout(timeoutList.get((int)(timeoutList.size()*0.95)).intValue());
		result.setBatchCode(tsParamConfig.getBatchCode());
		long sumPps=0;
		for(long pps:ppsList) {
			sumPps+=pps;
		}
		result.setPps((int)(sumPps/ppsList.size()));
		return result;
	}
	/**
	 * 生成数据
	 * @param currentTime
	 * @return
	 */
	private LinkedList<TsWrite> generatePkg(long currentTime) {
		int dn=tsParamConfig.getDeviceNum();//每个客户端对应的设备数
		int cn=tsParamConfig.getWriteClients();
		int sn=tsParamConfig.getSensorNum();
		int sdn=dn*cn;//总设备数
		long generateStartTime=currentTime;
		long generateEndTime=generateStartTime+tsParamConfig.getStep()*tsParamConfig.getCacheTimes();
		LinkedList<TsWrite> pkgs=new LinkedList<TsWrite>();
		for(int clientIndex=0;clientIndex<cn;clientIndex++) {
			TsWrite tsw=new TsWrite();
			for(int cdn=0;cdn<sdn;cdn++) {
				if(cdn%cn==clientIndex) {//生成数据并加入
					long currentTime0=generateStartTime;
					while(currentTime0<generateEndTime) {
						TsPackage tpc=new TsPackage();
						String deviceCode=devicePre+cdn;
						tpc.setDeviceCode(deviceCode);
						tpc.setTimestamp(currentTime0);
						for(int csn=0;csn<sn;csn++) {
							if(random.nextDouble()<tsParamConfig.getLoseRatio()) {
								continue;
							}
							String sensorCode=sensorPre+csn;
							tpc.appendValue(sensorCode, generateValue(deviceCode, sensorCode, currentTime0));
						}
						tsw.append(tpc);
						currentTime0+=tsParamConfig.getStep();
					}
				}
			}
			pkgs.add(tsw);
		}
		return pkgs;
	}
	
	
	/**
	 * 获取值
	 * @param deviceCode
	 * @param sensorCode
	 * @return
	 */
	public  Object generateValue(String deviceCode, String sensorCode,Long time) {
		long shirtTime=getShiftTimeByDeviceAndSensor(deviceCode,sensorCode);
		FunctionParam functionParam=getFunctionBySensor(sensorCode);
		return Function.getValueByFuntionidAndParam(functionParam.getFunctionType(), functionParam.getMax(), functionParam.getMin(), functionParam.getCycle(), time+shirtTime);
	}
	private FunctionParam getFunctionBySensor(String sensorCode) {
		return sensorFunctionMap.get(sensorCode);
	}
	private long getShiftTimeByDeviceAndSensor(String deviceCode, String sensorCode) {
		return shiftTimeMap.get(deviceCode+"_"+sensorCode);
	}
	public TsReadResult queryTest() {
		int csn = tsParamConfig.getReadClients();
		ExecutorService pool = Executors.newFixedThreadPool(csn+1);
		CompletionService<Long[]> cs = new ExecutorCompletionService<Long[]>(pool);
		long startTime=System.nanoTime();
		long programStartTime=System.currentTimeMillis();
//		List<Long> timeoutList=new ArrayList<>();
		List<Long> timeoutList=new Vector<>();
		for(int clientIndex=0;clientIndex<csn;clientIndex++) {
			cs.submit(new Callable<Long[]>() {
				@Override
				public Long[] call() throws Exception {
					if(tsParamConfig.getBackgroupStatus().equals(1)) {
						int sleepTime = random.nextInt((int)tsParamConfig.getReadPulse())+1;
						Thread.sleep(sleepTime);
//						LOGGER.info("sleep time "+sleepTime+" ms");
					}
					Long[] results=new Long[3];
					results[0]=0L;
					results[1]=0L;
					results[2]=0L;
					long endTime=System.currentTimeMillis()+TimeUnit.SECONDS.toMillis(tsParamConfig.getReadPeriod());
					Long bizStartTime;
//					DBAdapter newDbAdapter = dbAdapter.getClass().newInstance();
					while((bizStartTime=System.currentTimeMillis())<endTime) {//结束时间之前，一直执行
						TsQuery query = generateTsQuery();
						Status status = execQuery(dbAdapter, query);
						if(status.isOK()) {
							results[0]+=status.getCostTime()/1000;//us
							results[1]=results[1]+1;
							timeoutList.add(status.getCostTime()/1000);//us
						}else {
							results[2]=results[2]+1;
						}
						long bizEndTime=System.currentTimeMillis();
						long bizCost=bizEndTime-bizStartTime;
						if(bizCost<tsParamConfig.getReadPulse()) {//每隔 ReadPulse ms发送一次请求
							Thread.sleep(tsParamConfig.getReadPulse());
						}
					}
//					newDbAdapter.closeAdapter();
					return results;
				}
			});
		}
		pool.execute(new Runnable() {
			@Override
			public void run() {
				long endTime=System.currentTimeMillis()+TimeUnit.SECONDS.toMillis(tsParamConfig.getReadPeriod());
				long actStartTime=System.currentTimeMillis();
				while(System.currentTimeMillis()<endTime) {//结束时间之前，一直执行 
					int bottle=10;//计时间隔
					try {
						Thread.sleep(bottle*1000L);
					} catch (Exception e) {
						e.printStackTrace();
					}
					long bizSumCost=System.currentTimeMillis()-actStartTime;
					LOGGER.info("read operate has cost time [{} s],progress [{}/{}]",bizSumCost/1000,bizSumCost/1000,tsParamConfig.getReadPeriod());
				}
			}
		});
		long sumTimeout=0L;
		long sumSuccessNum=0L;
		long failedSum=0L;
		TsReadResult result=new TsReadResult();
		try {
			for(int index=0;index<csn;index++) {
				Long[] results;
					results = cs.take().get();
					sumTimeout+=results[0];
					sumSuccessNum+=results[1];
					failedSum+=results[2];
//					timeoutList.add(results[0]);
			}
			long costTime=System.nanoTime()-startTime;
			int tps=(int) (sumSuccessNum/(costTime/Math.pow(10, 9)));
			long avgtimeout=sumTimeout/sumSuccessNum;
			LOGGER.info("clients {},throughput [{} requests/sec],mean timeout [{} us],successNum [{}/{}]",csn,tps,avgtimeout,sumSuccessNum,failedSum+sumSuccessNum);
			//返回数据处理
			result.setBatchCode(tsParamConfig.getBatchCode());
			result.setStartTime(programStartTime);
			result.setEndTime(System.currentTimeMillis());
			result.setMeanTimeout((int)avgtimeout);
			result.setTps(tps);
			result.setSumRequests(sumSuccessNum);
			result.setSuccessRatio((double)sumSuccessNum/(failedSum+sumSuccessNum));
			Collections.sort(timeoutList);
			result.setMaxTimeout(timeoutList.get( timeoutList.size()-1).intValue());
			result.setMinTimeout(timeoutList.get(0).intValue());
			result.setNinty5Timeout(timeoutList.get((int)(timeoutList.size()*0.95)).intValue());
			result.setFiftyTimeout(timeoutList.get((int)(timeoutList.size()*0.5)).intValue());
		} catch (Exception e) {
			e.printStackTrace();
		}
		pool.shutdown();
		return result;
	}
	private TsQuery generateTsQuery() throws Exception {
		TsQuery query=new TsQuery();
		query.setDeviceName("d_"+random.nextInt(tsParamConfig.getDeviceNum()));
		query.setSensorName("s_"+random.nextInt(tsParamConfig.getSensorNum()));
		long startTime=tsParamConfig.getStartTime();
		long endTime=tsParamConfig.getEndTime();
		TimeSlot timeslot=null;
		double queryType = random.nextDouble();
		if(queryType<tsParamConfig.getReadSimpleRatio()) {
			query.setAggreType(1);
			//可以加上比较值 按照几率
			timeslot = TSUtils.getRandomTimeBetween(startTime, endTime, TimeUnit.HOURS.toMillis(1));
		}else if(queryType<tsParamConfig.getReadSimpleRatio()+tsParamConfig.getReadAggreRatio()) {
			query.setQueryType(2);
			query.setAggreType(random.nextInt(3)+1);
			timeslot = TSUtils.getRandomTimeBetween(startTime, endTime, TimeUnit.HOURS.toMillis(1));
		}else if(queryType<tsParamConfig.getReadSimpleRatio()+tsParamConfig.getReadAggreRatio()+tsParamConfig.getReadShrinkRatio()){//一小时每分钟内的最大最小平均值
			query.setQueryType(2);
			query.setAggreType(random.nextInt(3)+1);
			query.setGroupByUnit(2);
			timeslot = TSUtils.getRandomTimeBetween(startTime, endTime, TimeUnit.HOURS.toMillis(1));
		}else{//多个设备同一传感器聚合  1h (10个)
			query.setQueryType(3);
			query.setAggreType(random.nextInt(3)+1);
			query.setDeviceName("*");
			List<String> devices=generateDeviceCodes(10,100);
			query.setDevices(devices);
			timeslot = TSUtils.getRandomTimeBetween(startTime, endTime, TimeUnit.HOURS.toMillis(1));
		}
		query.setStartTimestamp(timeslot.getStartTime());
		query.setEndTimestamp(timeslot.getEndTime());
		return query;
	}
	
	
	
	public static List<String> generateDeviceCodes(int num,int sum) {
		Random random=new Random();
		List<String> list = new ArrayList<String>();
		String template="d_%s";
		while(list.size()<num){
			int nextInt = random.nextInt(sum);
			String deviceCode=String.format(template,nextInt);
			if(!list.contains(deviceCode)){
				list.add(deviceCode);
			}
		}
		return list;
	}
	public static Status execQuery(DBAdapter dbAdapter,TsQuery query) {
		return dbAdapter.execQuery(dbAdapter.preQuery(query));
	}
	public static Status execWrite(DBAdapter dbAdapter,TsWrite write) {
		return dbAdapter.execWrite(dbAdapter.preWrite(write));
	}
	public CoreBiz(TsParamConfig tsParamConfig, TsDataSource tds) throws Exception {
		super();
		Core.initInnerFucntion();
		this.tsParamConfig = tsParamConfig;
		this.dbAdapter = (DBAdapter) Class.forName(tds.getDriverClass()).newInstance();
		String batchCode=String.format("%s_%s_%s_",tds.getBatchCode(),tds.getDbType(),tsParamConfig.getTestMode());
		if("read".equals(tsParamConfig.getTestMode())) {
			batchCode+=tsParamConfig.getReadClients();
		}
		if("write".equals(tsParamConfig.getTestMode())) {
			batchCode+=tsParamConfig.getWriteClients();
		}
		this.tsParamConfig.setBatchCode(batchCode);
		dbAdapter.initDataSource(tds,tsParamConfig);
		initShiftTime();
		initSensorFunction();
	}
	private void initShiftTime() {
		int dn=tsParamConfig.getDeviceNum();
		int sdn=dn*tsParamConfig.getWriteClients();
		int sn=tsParamConfig.getSensorNum();
		long sensorSum=sdn*sn;
		for(int cdn=0;cdn<sdn;cdn++) {
			for(int csn=0;csn<sn;csn++) {
				shiftTimeMap.put(devicePre+cdn+"_"+sensorPre+csn, (long)(random.nextDouble()*sensorSum));
			}
		}
	}
	private void initSensorFunction() {
		//根据传进来的各个函数比例进行配置
		double constantRatio=tsParamConfig.getConstantRatio();
		double lineRatio=tsParamConfig.getLineRatio();
		double randomRatio=tsParamConfig.getRandomRatio();
		double sinRatio=tsParamConfig.getSinRatio();
		double squareRatio=tsParamConfig.getSquareRatio();
		int sn=tsParamConfig.getSensorNum();
		double sumRatio=constantRatio+lineRatio+randomRatio+sinRatio+squareRatio;
		if(sumRatio!=0
			&&constantRatio>=0
			&&lineRatio>=0
			&&randomRatio>=0
			&&sinRatio>=0
			&&squareRatio>=0){
			double constantArea=constantRatio/sumRatio;
			double lineArea=constantArea+lineRatio/sumRatio;
			double randomArea=lineArea+randomRatio/sumRatio;
			double sinArea=randomArea+sinRatio/sumRatio;
			double squareArea=sinArea+squareRatio/sumRatio;
			for(int i=0;i<sn;i++){
				double property = random.nextDouble();
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
				}
				String sensorCode=sensorPre+i;
				sensorFunctionMap.put(sensorCode,param);
				
			}
			
		}else{
			System.err.println("function ratio must >=0 and sum>0");
		}
	}
	public static void main(String[] args) {
	}
}
