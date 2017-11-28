package cn.edu.ruc.db;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import cn.edu.ruc.biz.Constants;


/**
 * 数据库操作基本接口
 * @Explaination
 * 每一个接口最后都是返回对象 Status
 * 如果操作数据库成功，
 * 写入成功调用 Status.OK(costTime) costTime为写入消耗ms
 * 查询成功调用 Status.OK(costTime,records) costTime为写入消耗ms,records为查询成功的数据点数
 * 修改成功调用 Status.OK(costTime) costTime为写入消耗ms
 * 删除成功调用 Status.OK(costTime) costTime为写入消耗ms
 * <br>
 * 如果操作数据库失败
 * 则调用Status.FAILED(costTime) costTime为消耗的时间
 * 
 * 
 * 
 * 
 * @author sxg
 */
public  abstract class DBBase {
	  /**
	   * Properties for configuring this DB.
	   */
	  private Properties properties = new Properties();

	  /**
	   * Set the properties for this DB.
	   */
	  public void setProperties(Properties p) {
	    properties = p;

	  }
	  /**
	   * Get the set of properties for this DB.
	   */
	  public Properties getProperties() {
	    return properties;
	  }
	  /**
	   * Initialize any state for this DB.
	   * Called once per DB instance; there is one DB instance per client thread.
	   */
	  public void init() {
	  }

	  /**
	   * Cleanup any state for this DB.
	   * Called once per DB instance; there is one DB instance per client thread.
	   */
	  public void cleanup() {
	  }
	  /**
	   * 获取数据库服务器ip+端口号
	   * 唯一标识这个数据库服务器
	   * @return
	   */
	  public abstract String getDBUrl();
	  /**
	   * 数据生成器生成数据
	   * influxdb的数据生成器已经完成，别的可参考influxdb
	   */
	  public long generateData(){
		  return 0;
	  }
	  protected String getDataPath(){
	    	String path = System.getProperty("user.dir");
			File dir=new File(path+"/data");
			if(!dir.exists()){
				dir.mkdir();
			}
			return path+"/data";
	  }
	  /**
	   * 获取数据生成器一共生成多少次（每次100W条）
	   * @return
	   */
	  public int getSumTimes(){
		  return getSumTimes(1000000);//FIXME 具体怎么算可以优化
	  }
	  /**
	   * 获取数据生成器一共生成多少次
	   * @param lines 每次的生成条数
	   * @return
	   */
	  public int getSumTimes(int lines){
		  long startTime=Constants.HISTORY_START_TIME;
		  long endTime=Constants.HISTORY_END_TIME;
		  double step = (double)Constants.POINT_STEP;
		  int ds=Constants.DEVICE_NUMBER;
		  int ss=Constants.SENSOR_NUMBER;
		  long sum=(long) ((endTime-startTime)/step*ds*ss);
		  return (int) (sum/lines)+1;//需要加1，否则sum<lines时，无法生成数据
	  }
	  
	/**
	 * 向数据库中插入数据
	 * @param points 需要插入的数据   有序的，首先按照时间排序，然后按照设备号排序;
	 * @return
	 */
	public abstract Status insertMulti(List<TsPoint> points); 
	/**
	 * 1号查询 查询某个指定编号设备，指定编号传感器，在一段时间内的数据
	 * <br>
	 * 比如 查询28号风机，18号温度传感器在2017年6月3日到2017年6月4日的所有数据
	 * @param point  要查的传感器的信息 要查的传感器的信息 包含信息为设备编号，传感器编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return  
	 */
	public abstract Status selectByDeviceAndSensor(TsPoint point,Date startTime,Date endTime);
	/**
	 * 2号查询  查询某个指定编号设备，指定编号传感器，在一段时间内小于max，大于min的所有数据
	 * <br>
	 * 比如 查询28号风机，18号温度传感器在2017年6月3日到2017年6月4日的温度小于80摄氏度且大于20摄氏度的所有数据
	 * @param point  要查的传感器的信息 包含信息为设备编号，传感器编号
	 * @param max 传感器的值都小于max
	 * @param min 传感器的值都大于min
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return  
	 */
	public abstract Status selectByDeviceAndSensor(TsPoint point,Double max,Double min,Date startTime,Date endTime);
	/**
	 * 3号查询  查询某个指定编号设备所有传感器在某段时间的采集值
	 * <br>
	 * 比如 查询28号风机，所有传感器2017年6月3日到2017年6月4日所有的数据
	 * @param point 要查的传感器的信息 包含信息为设备编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return
	 */
	public abstract Status selectByDevice(TsPoint point,Date startTime,Date endTime);
	/**
	 * 查询某个指定编号设备所有传感器在某段时间,每天的最大值
	 * <br>
	 * 比如 查询25号风机， 2017年6月3日到2017年6月30日的所有传感器每天的最大值
	 * @param point 要查的传感器的信息 包含信息为设备编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return
	 */
	public abstract Status selectDayMaxByDevice(TsPoint point,Date startTime,Date endTime);
	/**
	 * 查询某个指定编号设备所有传感器在某段时间,每天的最小值
	 * <br>
	 * 比如 查询25号风机， 2017年6月3日到2017年6月30日的所有传感器每天的最小值
	 * @param point 要查的传感器的信息 包含信息为设备编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return
	 */
	public abstract Status selectDayMinByDevice(TsPoint point,Date startTime,Date endTime);
	/**
	 * 查询某个指定编号设备所有传感器在某段时间,每天的平均值
	 * <br>
	 * 比如 查询25号风机， 2017年6月3日到2017年6月30日的所有传感器每天的平均值
	 * @param point 要查的传感器的信息 包含信息为设备编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return
	 */
	public abstract Status selectDayAvgByDevice(TsPoint point,Date startTime,Date endTime);
	/**
	 * 查询某个指定编号设备所有传感器在某段时间,每个小时的最大值
	 * <br>
	 * 比如 查询25号风机， 2017年6月3日到2017年6月4日的所有传感器每个小时的最大值
	 * @param point 要查的传感器的信息 包含信息为设备编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return
	 */
	public abstract Status selectHourMaxByDevice(TsPoint point,Date startTime,Date endTime);
	/**
	 * 查询某个指定编号设备所有传感器在某段时间,每个小时的最小值
	 * <br>
	 * 比如 查询25号风机， 2017年6月3日到2017年6月4日的所有传感器每个小时的最小值
	 * @param point 要查的传感器的信息 包含信息为设备名称，设备编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return
	 */
	public abstract Status selectHourMinByDevice(TsPoint point,Date startTime,Date endTime);
	/**
	 * 查询某个指定编号设备所有传感器在某段时间,每个小时的平均值
	 * <br>
	 * 比如 查询25号风机， 2017年6月3日到2017年6月4日的所有传感器每个小时的平均值
	 * @param point 要查的传感器的信息 包含信息为设备编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return
	 */
	public abstract Status selectHourAvgByDevice(TsPoint point,Date startTime,Date endTime);
	/**
	 * 查询某个指定编号设备，指定编号传感器在某段时间,每分钟的最大值
	 *  <br>
	 * 比如 查询25号风机， 2017年6月3日到2017年6月4日的8号温度传感器每分钟的最大值
	 * @param point  要查的传感器的信息 包含信息为设备编号，传感器编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return
	 */
	public abstract Status selectMinuteMaxByDeviceAndSensor(TsPoint point,Date startTime,Date endTime);
	/**
	 * 查询某个指定编号设备，指定编号传感器在某段时间,每分钟的最小值
	 * <br>
	 * 比如 查询25号风机， 2017年6月3日到2017年6月4日的8号温度传感器每分钟的最小值
	 * @param point  要查的传感器的信息 包含信息为设备编号，传感器编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间 
	 * @return
	 */
	public abstract Status selectMinuteMinByDeviceAndSensor(TsPoint point,Date startTime,Date endTime);
	/**
	 * 查询某个指定编号设备，指定编号传感器在某段时间,每分钟的平均值
	 * <br>
	 * 比如 查询25号风机， 2017年6月3日到2017年6月4日的8号温度传感器每分钟的平均值
	 * @param point  要查的传感器的信息 包含信息为设备编号传感器编号
	 * @param startTime 开始时间
	 * @param endTime 结束时间
	 * @return
	 */
	public abstract Status selectMinuteAvgByDeviceAndSensor(TsPoint point,Date startTime,Date endTime);
	
	/**
	 * 更新数据
	 * @param points  需要修改的数据点
	 * @return
	 */
	public abstract Status updatePoints(List<TsPoint> points);
	/**
	 * 删除某个时间点之前的所有数据
	 * @param date 日期
	 * @return
	 */
	public abstract Status deletePoints(Date date);
	/**
	 * 查询指定设备，指定传感器，在指定开始时间和结束时间之间的最大值
	 * @param deviceCode
	 * @param sensorCode
	 * @return
	 */
	public abstract Status selectMaxByDeviceAndSensor(String deviceCode,String sensorCode,Date startTime,Date endTime);
	/**
	 * 查询指定设备，指定传感器，在指定开始时间和结束时间之间的最小值
	 * @param deviceCode
	 * @param sensorCode
	 * @return
	 */
	public abstract Status selectMinByDeviceAndSensor(String deviceCode,String sensorCode,Date startTime,Date endTime);
	/**
	 * 查询指定设备，指定传感器，在指定开始时间和结束时间之间的平均值
	 * @param deviceCode
	 * @param sensorCode
	 * @return
	 */
	public abstract Status selectAvgByDeviceAndSensor(String deviceCode,String sensorCode,Date startTime,Date endTime);
	/**
	 * 查询指定设备，指定传感器，在指定开始时间和结束时间之间的记录数目
	 * @param deviceCode
	 * @param sensorCode
	 * @return
	 */
	public abstract Status selectCountByDeviceAndSensor(String deviceCode,String sensorCode,Date startTime,Date endTime);
}

