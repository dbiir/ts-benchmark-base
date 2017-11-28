package cn.edu.ruc.biz;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import cn.edu.ruc.TSUtils;
import cn.edu.ruc.db.DBBase;
import cn.edu.ruc.db.Status;
import cn.edu.ruc.db.TsPoint;

/**
 * 压力测试
 * @author sxg
 */
public class StressAppend {
	private static final int THREAD_NUMBER=50;
	
	/**设备编号*/
	private static final List<String> DEVICE_CODES=Constants.DEVICE_CODES;
	/**传感器编号*/
	private static final List<String> SENSOR_CODES=Constants.SENSOR_CODES; 
	private static final Map<String,Long> SHIFT_TIME_MAP=Constants.SHIFT_TIME_MAP; 
	public static void main(String[] args) {
		Core.initInnerFucntion();
		Properties props = Core.parseArguments(args);
		int sn=500;
		initSensorCodes(sn);
		initSensorFunction();
		Core.initConstant(props);
		startStressAppend();
	}
	private static long IMPORT_COST_TIME=0;
	private static int SUM_THREAD_POINTS=0;
	public static void startStressAppend(){
		
		//1,生成dn个 设备(每个设备500个传感器)的7min的数据
		//dn=threads*k k>0;
		
		
//		int sn=500;
//		initSensorCodes(sn);
//		initSensorFunction();
		int seq=1;
		long start=System.currentTimeMillis();
		while(seq*THREAD_NUMBER<10000){
			long start1=System.currentTimeMillis();
			//生成数据
//			Map<Integer,List<List<TsPoint>>> dataMap= generate7MinData(seq,THREAD_NUMBER);
			final Map<Integer,TsPoint[][]> dataMap= generate7MinData(seq,THREAD_NUMBER);//用数组存储效率虽然占用内存多一些，但是运行效率高些，空间换时间
			long end1=System.currentTimeMillis();
			System.out.println("device number="+seq*THREAD_NUMBER+",generate data costTime:["+(end1-start1)+" ms]");
			DBBase dbBase= Constants.getDBBase();
			int startTime=0;
//			int sumThreadPoints=0;
			long avgPointsRatio=0;
			long avgTimeout=0;
			while(startTime<420){
				SUM_THREAD_POINTS=0;
				IMPORT_COST_TIME=0;
				TsPoint[][] tsPoints = dataMap.get(startTime);
				//开辟线程写入数据
				ExecutorService pool = Executors.newFixedThreadPool(THREAD_NUMBER);
				long startTimeLoad=System.currentTimeMillis();
				for(int threadIndex=0;threadIndex<THREAD_NUMBER;threadIndex++){
					final List<TsPoint> points = new ArrayList<TsPoint>(Arrays.asList(tsPoints[threadIndex]));
//					SUM_THREAD_POINTS+=getPointSize(points);
					pool.execute(new Runnable() {
						@Override
						public void run() {
							Status status = dbBase.insertMulti(points);
							if(status.isOK()){
								synchronized (StressAppend.class) {
									long costTime = status.getCostTime();
									IMPORT_COST_TIME+=costTime;
									SUM_THREAD_POINTS+=getPointSize(points);
								}
							}else{
								System.out.println("status failed");
							}
						}
					});
				}
				startTime+=7;
				pool.shutdown();
				try {
					pool.awaitTermination(15, TimeUnit.MINUTES);
					long endTimeLoad=System.currentTimeMillis();
					long avgCostTime=(long) ((endTimeLoad-startTimeLoad)*Math.pow(1000, 2));
					int pointsRatio=(int)(SUM_THREAD_POINTS/(avgCostTime/Math.pow(1000.0, 3)));
					int timeout=(int) (IMPORT_COST_TIME/(double)SUM_THREAD_POINTS/Math.pow(1000, 1));//每个点的延迟时间，单位是us
//					System.out.println("order["+startTime/7+"/"+420/7+"]import["+SUM_THREAD_POINTS+"]points,cost ["+TimeUnit.NANOSECONDS.toMillis(avgCostTime) +" ms]，import speed["+pointsRatio+" points/s],timeout["
//							+timeout
//							+" us/point]");
					avgPointsRatio=(avgPointsRatio+pointsRatio)/2;
					avgTimeout=(avgTimeout+timeout)/2;
					Thread.sleep(300L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.println("device number="+seq*THREAD_NUMBER+",avg load speed="+avgPointsRatio+" points/s,avg timeout="+avgTimeout+"");
			if(seq<=20){
				seq*=2;//3
			}else{
				seq+=20;//3
			}
		}
		long end=System.currentTimeMillis();
		System.out.println("costTime:"+(end-start));
	}
	private static int getPointSize(List<TsPoint> points) {
		Iterator<TsPoint> iterator = points.iterator();
		int count=0;
		while(iterator.hasNext()){
			TsPoint next = iterator.next();
			if(next!=null){
				count++;
			}
		}
		return count;
	}
	/**
	 * 
	 * @param seq
	 * @param threadNumber
	 * @return map time-seq---->list(threadNumber个脚标，每个里面对应相应线程的数据)
	 */
//	private static Map<Integer,List<List<TsPoint>>> generate7MinData(int seq, int threadNumber) {
	private static Map<Integer,TsPoint[][]> generate7MinData(int seq, int threadNumber) {
		int dn=seq*threadNumber;
		int sumTime=420;//420s的数据
		int step=7;//7s;
		double loseRatio=0.0001;//数据丢失率
//		Map<Integer,List<List<TsPoint>>> dataMap=initDataMap(sumTime,threadNumber);
		Map<Integer, TsPoint[][]> dataMap=initDataMap(sumTime,threadNumber);
		initDeviceCodes(dn);
		initShiftTime();
		Random r=new Random();
		Calendar c=Calendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.MINUTE,-7);
		long currentTime=c.getTimeInMillis(); //当前时间减去7分钟
		for(int mapKey=0;mapKey<sumTime;mapKey+=step){
			currentTime=currentTime+mapKey*step*1000;//模拟当前时间
//			List<List<TsPoint>> indexList = dataMap.get(mapKey);
			TsPoint[][] tsPoints = dataMap.get(mapKey);
			for(int dnIndex=0;dnIndex<dn;dnIndex++){
				int threadId=dnIndex%threadNumber;
//				List<TsPoint> points = indexList.get(threadId);
//				List<TsPoint> points = tsPointsthreadId);
				String deviceCode=DEVICE_CODES.get(dnIndex);
				for(int sensorNum=0;sensorNum<500;sensorNum++){//500为sensor总数
					double randomFloat = r.nextDouble();
					if(randomFloat<(1-loseRatio)){
						TsPoint point=new TsPoint();
						point.setDeviceCode(deviceCode);
						String sensorCode = SENSOR_CODES.get(sensorNum);
						point.setSensorCode(sensorCode);
						point.setValue(getValue(deviceCode,sensorCode,currentTime));
						point.setTimestamp(currentTime);
						tsPoints[threadId][sensorNum+(dnIndex/threadNumber)*500]=point;
//						if(threadId==1){
//							System.out.println(threadId+":"+(sensorNum+(dnIndex/threadNumber)*500));
//						}
//						TsPoint[] tp=tsPoints[threadId];
//						points.add(point);
					}
				}
//				System.out.println("threadId:"+threadId+"|deviceId:"+dnIndex+"|point.size:"+points.size());
			}
//			System.out.println(tsPoints);
//			System.out.println("mapkey:"+mapKey);
//			dataMap.put(mapKey, indexList);
			dataMap.put(mapKey, tsPoints);
		}
		return dataMap;
	}
	public static Object getValue(String deviceCode,String sensorCode,long currentTime){
		FunctionParam functionParam=Core.getFunctionBySensor(sensorCode);
		return Function.getValueByFuntionidAndParam(functionParam.getFunctionType(), functionParam.getMax(), functionParam.getMin(), functionParam.getCycle(),currentTime);
	}
	private static void initSensorFunction() {
		Core.initSensorFunction();
	}
	private static void initSensorCodes(int sn) {
		SENSOR_CODES.clear();
		for(int i=0;i<sn;i++){
			String sensorCode="s_"+TSUtils.getRandomLetter(3)+"_"+i;
			SENSOR_CODES.add(sensorCode);
		}
	}
	private static void initDeviceCodes(int dn) {
		DEVICE_CODES.clear();
		for(int i=0;i<dn;i++){
			String deviceCode=UUID.randomUUID().toString().split("-")[0];
			DEVICE_CODES.add(deviceCode);
		}
	}
	/**
	 * 初始化时间偏移量
	 */
	private static void initShiftTime() {
		SHIFT_TIME_MAP.clear();
		int step=7000;
		long sensorSum=DEVICE_CODES.size()*SENSOR_CODES.size();
		Random r=new Random();
		for(int i=0;i<DEVICE_CODES.size();i++){
			for(int j=0;j<SENSOR_CODES.size();j++){
				Long shiftTime=(long)(r.nextDouble()*sensorSum)*step;
				SHIFT_TIME_MAP.put(DEVICE_CODES.get(i)+"_"+SENSOR_CODES.get(j), shiftTime);
			}
		}
	}
	/**
	 * 所有的时间
	 * @param sumTime
	 * @return map time-seq---->list(threadNumber个脚标，每个里面对应相应线程的数据)
	 */
//	private static Map<Integer, List<List<TsPoint>>> initDataMap(int sumTime, int threadNumber) {
//		Map<Integer, List<List<TsPoint>>> dataMap=new HashMap<Integer, List<List<TsPoint>>>();
//		for(Integer i=0;i<sumTime;i++){
//			List<List<TsPoint>> indexList=new ArrayList<List<TsPoint>>();
//			for(int index=0;index<threadNumber;index++){
//				indexList.add(new LinkedList<TsPoint>());
//			}
//			dataMap.put(i, indexList);
//		}
////		TsPoint[] pointss=new TsPoint[]
//		return dataMap;
//	}
	private static Map<Integer, TsPoint[][]> initDataMap(int sumTime, int threadNumber) {
		Map<Integer, TsPoint[][]> dataMap=new HashMap<Integer, TsPoint[][]>();
		TsPoint[][] tsPoints=new TsPoint[threadNumber][100000];
		for(Integer i=0;i<sumTime;i++){
			dataMap.put(i*7, tsPoints);
		}
//		TsPoint[] pointss=new TsPoint[]
		return dataMap;
	}
}

