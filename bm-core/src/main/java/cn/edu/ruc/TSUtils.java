package cn.edu.ruc;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.text.StrTokenizer;

import cn.edu.ruc.biz.Constants;


/**
 * 工具类
 * @author sxg
 */
public class TSUtils {
	/**
	 * 根据字符串获取日期
	 * @param dateStr yyyy-MM-dd hh:mm:ss
	 * @return
	 */
	public static long getTimeByDateStr(String dateStr){
		return getDateByDateStr(dateStr).getTime();
	}
	public static long getTimeByDateStr(String dateStr,String format){
		return getDateByDateStr(dateStr,format).getTime();
	}
	/**
	 * 根据字符串获取日期
	 * @param dateStr yyyy-MM-dd hh:mm:ss
	 * @return
	 */
	public static Date getDateByDateStr(String dateStr){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date date=new Date();
		try {
			date=sdf.parse(dateStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return date;
	}
	/**
	 * 根据字符串获取日期
	 * @param dateStr 
	 * @param format 日期格式
	 * @return
	 */
	public static Date getDateByDateStr(String dateStr,String format){
		SimpleDateFormat sdf=new SimpleDateFormat(format);
		Date date=new Date();
		try {
			date=sdf.parse(dateStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return date;
	}
	/**
	 * 获取给定时间戳之前30天的时间戳
	 * @param time
	 * @return
	 */
	public static long getTimeBeforeDay(long time){
		Calendar c=Calendar.getInstance();
		c.setTimeInMillis(time);
		c.add(Calendar.DAY_OF_MONTH, -30);
		return c.getTimeInMillis();
	}
	private static final String ABC_XYZ="abcdefghijklmnofqfstuvwxyz";
	private static Random RANDOM=new Random();
	/**
	 * 随机生成num个字母
	 * @param num
	 * @return
	 */
	public static String getRandomLetter(int num){
		StringBuilder sc=new StringBuilder();
		for(int i=0;i<num;i++){
			int number=(int)(RANDOM.nextDouble()*26);
			char letter = ABC_XYZ.charAt(number);
			sc.append(letter);
		}
		return sc.toString();
	}
	/**
	 * 在startTime和endTime之间随机选择长度为millis毫秒的时间段
	 * @param startTime 开始时间
	 * @param endTime 开始时间
	 * @param millis  时间长度，单位为ms
	 * @return
	 */
	public static TimeSlot getRandomTimeBetween(String startDate,
			String endDate, long millis) throws Exception{
		long begin = getTimeByDateStr(startDate);
		long end = getTimeByDateStr(endDate);
		if(end-begin<millis){
			System.err.println("begin="+begin+",end="+end+",millis="+millis+",end-begin<milils");
			throw new Exception("end-begin<milils");
		}
		long virEnd=end-millis;
		Random r=new Random();
		long startTime= (long)(r.nextDouble()*(virEnd-begin)+begin);
		long endTime=startTime+millis;
		return new TimeSlot(startTime, endTime);
	}
	/**
	 * 在startTime和endTime之间随机选择长度为millis毫秒的时间段
	 * @param startTime 开始时间
	 * @param endTime 开始时间
	 * @param millis  时间长度，单位为ms
	 * @return
	 */
	public static TimeSlot getRandomTimeBetween(long begin,
			long end, long millis) throws Exception{
		if(end-begin<millis){
			System.err.println("begin="+begin+",end="+end+",millis="+millis+",end-begin<milils");
			throw new Exception("begin-end<milils");
		}
		long virEnd=end-millis;
		Random r=new Random();
		long startTime= (long)(r.nextDouble()*(virEnd-begin)+begin);
		long endTime=startTime+millis;
		return new TimeSlot(startTime, endTime);
	}
	/**
	 * 在startTime和endTime之间随机选择长度为millis毫秒的时间段，并且(开始时间-begin)%mod==0
	 * @param startTime 开始时间
	 * @param endTime 开始时间
	 * @param millis  时间长度，单位为ms
	 * @return
	 */
	public static TimeSlot getRandomModTimeBetween(long begin,
			long end, long millis,long mod) throws Exception{
		if(end-begin<millis){
			System.err.println("begin="+begin+",end="+end+",millis="+millis+",end-begin<milils");
			throw new Exception("begin-end<milils");
		}
		long virEnd=end-millis;
		Random r=new Random();
		long startTime= (long)(r.nextDouble()*(virEnd-begin)+begin);
		startTime=((startTime-begin)/mod*mod)+begin;
		long endTime=startTime+millis;
		return new TimeSlot(startTime, endTime);
	}
	public static TimeSlot getRandomTimeBetween(long millis){
		long begin=Constants.HISTORY_START_TIME;
		long end=Constants.HISTORY_END_TIME;
		long virEnd=end-millis;
		Random r=new Random();
		long startTime= (long)(r.nextDouble()*(virEnd-begin)+begin);
		long endTime=startTime+millis;
		return new TimeSlot(startTime, endTime);
	}
}

