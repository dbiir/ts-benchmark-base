package cn.edu.ruc.biz;

import java.util.Date;
import java.util.Random;

import cn.edu.ruc.TSUtils;

public class Function {
	private final static double[] ABNORMAL_RATE={0.005,0.01,0.1,0.15,0.2};
	private final static long RELATIVE_ZERO_TIME=TSUtils.getTimeByDateStr("2016-01-13 00:00:00");
	/**
	 * 获取带有噪点的值,并且带浮动值，上下浮动value*0.005
	 * @param value
	 * @return
	 */
	public static double getAbnormalPoint(double value){
		value=value*(1+(RANDOM.nextDouble()/100-0.005));
		if(RANDOM.nextDouble()<ABNORMAL_RATE[0]){
			value=value*(1+(RANDOM.nextDouble()-0.5));
		}
		return value;
	}
	/**
	 * 获取单调函数浮点值
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为s
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static double getMonoValue(double max,double min,double cycle,long currentTime){
		double k=(max-min)/(cycle*1000);
		return k*(currentTime%1000000);
	}
	/**
	 * 获取单调函数浮点值
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为s
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static double getMonoKValue(double max,double min,double cycle,long currentTime){
		double k=(max-min)/(cycle);
		return min+k*currentTime/1000;
	}
	/**
	 * 
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为s
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static long getMonoValue(long max,long min,double cycle,long currentTime){
		double k=(max-min)/(cycle*1000);
		return (long)(k*(currentTime%(cycle*1000)));
	}
	
	/**
	 * 获取正弦函数浮点值
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为s
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static double getSineValue(double max,double min,double cycle,long currentTime){
		double w=2*Math.PI/(cycle*1000);
		double a=(max-min)/2;
		double b=(max-min)/2;
		return Math.sin(w*(currentTime%(cycle*1000)))*a+b+min;
	}
	/**
	 * 获取方波函数浮点值
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为s
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static double getSquareValue(double max,double min,double cycle,long currentTime){
		double t = cycle/2*1000;
		if((currentTime%(cycle*1000))<t){
			return max;
		}else{
			return min;
		}
	}
	private static final Random RANDOM=new Random();
	/**
	 * 获取随机数函数浮点值
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为s
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static double getRandomValue(double max,double min){
		return RANDOM.nextDouble()*(max-min)+min;
	}
	public static void main(String[] args) throws InterruptedException {
		for(int i=0;i<1000;i++){
			Thread.currentThread().sleep(1000L);
//			System.out.println(getMonoValue(1000L, 0L, 1000,System.currentTimeMillis()));
//			System.out.println(getSineValue(1000L, 0L, 20,System.currentTimeMillis()));
//			System.out.println(getSquareValue(1000L, 0L, 20,System.currentTimeMillis()));
//			System.out.println(getRandomValue(1000L, 0L));
		}
	}
	public static Number getValueByFuntionidAndParam(String functionId,Double max,Double min,
			long cycle,Long currentTime){
		
		currentTime=currentTime-RELATIVE_ZERO_TIME;
		if("float-sin".equals(functionId)){
			return (float)getSineValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		if("float-mono".equals(functionId)){
			return (float)getMonoValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		if("float-random".equals(functionId)){
			return (float)getRandomValue(max.doubleValue(), min.doubleValue());
		}
		if("float-square".equals(functionId)){
			return (float)getSquareValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		
		if("float-mono-k".equals(functionId)){
			return (float)getMonoKValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		
		if("double-sin".equals(functionId)){
			return getSineValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		if("double-mono".equals(functionId)){
			return getMonoValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		if("double-random".equals(functionId)){
			return getRandomValue(max.doubleValue(), min.doubleValue());
		}
		if("double-square".equals(functionId)){
			return getSquareValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		if("double-mono-k".equals(functionId)){
			return getMonoKValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		
		if("int-sin".equals(functionId)){
			return (int)getSineValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		if("int-mono".equals(functionId)){
			return (int)getMonoValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		if("int-random".equals(functionId)){
			return (int)getRandomValue(max.doubleValue(), min.doubleValue());
		}
		if("int-square".equals(functionId)){
			return (int)getSquareValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		if("int-mono-k".equals(functionId)){
			return (int)getMonoKValue(max.doubleValue(), min.doubleValue(), cycle, currentTime);
		}
		
		return 0;
	}
}
