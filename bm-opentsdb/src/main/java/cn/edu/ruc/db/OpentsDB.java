package cn.edu.ruc.db;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.alibaba.fastjson.JSON;

import cn.edu.ruc.biz.Core;
import entity.PointPut;
import entity.PointPutTag;

/**
 * Hello world!
 *
 */
public class OpentsDB extends DBBase
{
	private static final String DB_URL_OPENTSDB_PROPERTY="OpenTSDB.url";
	private String URL="";
	private String PUT_URL="/api/put";
	private String QUERY_URL="/api/query";
    public static void main( String[] args ) throws Exception
    {  
    	Core.main(args);
    }
    
    @Override
    public void init() {  // 形成url
    	super.init();  
    	Properties prop = getProperties();
    	URL = prop.getProperty(DB_URL_OPENTSDB_PROPERTY,"http://192.168.31.128:4242");
    	PUT_URL=URL+PUT_URL;//"http://192.168.31.128:4242/api/put";
    	QUERY_URL=URL+QUERY_URL;
    	HttpPoolManager.init();
 
    } 
    
	@Override
	public String getDBUrl() {
		// TODO Auto-generated method stub
		/*
		 * 获取数据库服务器ip+端口号 唯一标识这个数据库服务器
		 */
		Properties prop = getProperties();
	  	URL = prop.getProperty(DB_URL_OPENTSDB_PROPERTY,"http://192.168.31.128:4242");
		return URL;
	}


    //连接数据库
	private HttpClient getHttpClient(){
//		HttpClientBuilder hb = HttpClientBuilder.create();
//		RequestConfig config=RequestConfig.custom().setConnectTimeout(5000).setConnectionRequestTimeout((int)TimeUnit.HOURS.toMillis(1)).setSocketTimeout((int)TimeUnit.HOURS.toMillis(1)).build();
//		hb.setDefaultRequestConfig(config);
//		HttpClient hc = hb.build();
//		return hc;
//疑惑：既然有HttpClient这个类了，为什么不直接建立一个HttpClient的对象，new HttpClient hc? 
//config 里面的参数含义跟光哥确认？
		return HttpPoolManager.getHttpClient();
		
	}
	//insert 部分将转换后的json格式数据插入数据库
	private Status insertByHttpClient(String data){
		HttpClient hc = getHttpClient();
		HttpPost post=new HttpPost(PUT_URL);
		HttpResponse response=null;
		long costTime=0L;
		try {
			HttpEntity entity = new StringEntity(data);
//这是将json格式进一步封装成？
			post.setEntity(entity);
//post.set操作后，是不是说向数据库操作的所有操作（有哪些呢？）都已将准备完毕？只等一声令下？
			
			long startTime = System.nanoTime();
			response = hc.execute(post);
			long endTime = System.nanoTime();
			costTime=endTime-startTime; 
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(-1);
		}
		int statusCode=response.getStatusLine().getStatusCode();
		if (statusCode>=200&&statusCode<300) {
			return Status.OK(costTime);
		}else{
			return Status.FAILED(costTime);
		}
	}
	
	
	@Override
	public Status insertMulti(List<TsPoint> points) {
		ArrayList<PointPut> list2 = new ArrayList<PointPut>();
		if(points!=null){
			 for(TsPoint point :points) {  //points赋值
				 	if(point==null){
				 		break;
				 	}
			        PointPut pointPut=new PointPut();
			        pointPut.setTimestamp(""+point.getTimestamp());
		 	        pointPut.setValue(point.getValue());
			        PointPutTag tag=new PointPutTag(point);//变量host和变量dc所对应的值，复制
			        pointPut.setTags(tag);
			        list2.add(pointPut); //中间的			        
				}
		}
		String json =JSON.toJSONString(list2);//调用这个工具JSON.toJSONString将输入的数据转换为json格式
		return insertByHttpClient(json);	    
	}

	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Date startTime,
			Date endTime) {
		/*
		 * 查询某个指定编号设备，指定编号传感器，在一段时间内的数据 
        比如 查询28号风机，18号温度传感器在2017年6月3日到2017年6月4日的所有数据
		 */
	    Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",startTime.getTime());
		map.put("end", endTime.getTime());
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator", "sum");
		subQuery.put("metric", "time.series.perform");
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put("host",point.getDeviceCode());
		subTag.put("dc",point.getSensorCode());
		subQuery.put("tags", subTag);
		
//
		List<Map<String,Object>> list = new ArrayList<>();
		list.add(subQuery);
		map.put("queries",list);
		String json=JSON.toJSONString(map);		
		return queriesByHttpClient(json);
	}
	private Status queriesByHttpClient(String data){
		HttpClient hc = getHttpClient();
		HttpPost post=new HttpPost(QUERY_URL);
		HttpResponse response=null;
		long costTime=0L;
		try {
			HttpEntity entity = new StringEntity(data);
			post.setEntity(entity);
			long startTime = System.nanoTime();
			response = hc.execute(post);
			long endTime = System.nanoTime();
			costTime=endTime-startTime;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(-1);
		}
		int statusCode=response.getStatusLine().getStatusCode();
		if (statusCode>=200&&statusCode<300) {
			return Status.OK(costTime);
		}else{
			return Status.FAILED(costTime);
		}
	}
	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Double max,
			Double min, Date startTime, Date endTime) {

		//FIXME 当前版本2.3，目前是没有看到相关资料能通过httpAPI来实现此功能的。
		
		return null;
	}

	@Override
	public Status selectByDevice(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		//比如 查询28号风机，所有传感器2017年6月3日到2017年6月4日所有的数据
		/*
		 * 此为查询所有传感器，相比于上面的指定传感器，此处的SensorCode，设置为*即可
		 */
		  Map<String,Object> map = new  HashMap<String,Object>();
			map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "sum");
			subQuery.put("metric", "time.series.perform");
			
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			subTag.put("dc","*");//此处的SensorCode，设置为*
			subQuery.put("tags", subTag); 
			
			List <Map<String,Object>>list = new ArrayList<>();
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);
	}

	@Override
	public Status selectDayMaxByDevice(TsPoint point, Date startTime,
			Date endTime) {
		  Map<String,Object> map = new  HashMap<String,Object>();
			map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "max");
			subQuery.put("metric", "time.series.perform");
			//增加downsample
			subQuery.put("downsample", "24h-max");
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			//此处的SensorCode，设置为*
			subTag.put("dc","*");
			subQuery.put("tags", subTag);  //将此注释掉,默认应该是会选择所有的
			List<Map<String,Object>> list = new ArrayList<>();//最外层的[]
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);
		
	}

	@Override
	public Status selectDayMinByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		/* 第5个select
		 * 查询某个指定编号设备所有传感器在某段时间,每天的最小值 
 比如 查询25号风机， 2017年6月3日到2017年6月30日的所有传感器每天的最小值
 	 		每个传感器每天的最小值，每个传感器.
 	 		每天中的某一天的最小值用"downsample", "24h-min"来选择
		 */
		
		 Map<String,Object> map = new  HashMap<String,Object>();
			map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "min");
			subQuery.put("metric", "time.series.perform");
			subQuery.put("downsample", "24h-min");
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			subTag.put("dc","*");		
			List<Map<String,Object>> list = new ArrayList<>();
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);
	}

	@Override
	public Status selectDayAvgByDevice(TsPoint point, Date startTime,
			Date endTime) {
			Map<String,Object> map = new  HashMap<String,Object>();
			map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "avg");
			subQuery.put("metric", "time.series.perform");
			subQuery.put("downsample", "24h-avg");
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			subTag.put("dc","*");	
			List<Map<String,Object>> list = new ArrayList<>(); 
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);
	}

	@Override
	public Status selectHourMaxByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		/*查询某个指定编号设备所有传感器在某段时间,每个小时的最大值 
 比如 查询25号风机， 2017年6月3日到2017年6月4日的所有传感器每个小时的最大值
		 *  
		 *  只是将上面的downsample的单位改为1h
		 */
		 Map<String,Object> map = new  HashMap<String,Object>();
			map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "max");
			subQuery.put("metric", "time.series.perform");
			subQuery.put("downsample", "1h-max");
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			subTag.put("dc","*");	
			List<Map<String,Object>> list = new ArrayList<>(); 
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);
	}

	@Override
	public Status selectHourMinByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		/*
		 *查询某个指定编号设备所有传感器在某段时间,每个小时的最小值 
 比如 查询25号风机， 2017年6月3日到2017年6月4日的所有传感器每个小时的最小值
		 */
		 Map<String,Object> map = new  HashMap<String,Object>();
	
			map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "min");
			subQuery.put("metric", "time.series.perform");
			subQuery.put("downsample", "1h-min");
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			subTag.put("dc","*");	
			List<Map<String,Object>> list = new ArrayList<>(); 
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);
	}

	@Override
	public Status selectHourAvgByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		/*
		 * 查询某个指定编号设备所有传感器在某段时间,每个小时的平均值 
 比如 查询25号风机， 2017年6月3日到2017年6月4日的所有传感器每个小时的平均值
		 */
		 Map<String,Object> map = new  HashMap<String,Object>();
			map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "avg");
			subQuery.put("metric", "time.series.perform");
			subQuery.put("downsample", "1h-avg");
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			subTag.put("dc","*");	
			List<Map<String,Object>> list = new ArrayList<>(); 
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);
	}

	@Override
	public Status selectMinuteMaxByDeviceAndSensor(TsPoint point,
			Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		/*
		 * 查询某个指定编号设备，指定编号传感器在某段时间,每分钟的最大值 
 比如 查询25号风机， 2017年6月3日到2017年6月4日的8号温度传感器每分钟的最大值
		 */
//指定设备还指定了传感器，应该与上面的会有差别了,应该就不需要filter了
		 Map<String,Object> map = new  HashMap<String,Object>();
		 	map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "max");
			subQuery.put("metric", "time.series.perform");
			subQuery.put("downsample", "1m-max");
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			subTag.put("dc",point.getSensorCode());
			List<Map<String,Object>> list = new ArrayList<>(); 
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);	
	}

	@Override
	public Status selectMinuteMinByDeviceAndSensor(TsPoint point,
			Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		/*查询某个指定编号设备，指定编号传感器在某段时间,每分钟的最小值 
 比如 查询25号风机， 2017年6月3日到2017年6月4日的8号温度传感器每分钟的最小值
		 * 
		 */
		 Map<String,Object> map = new  HashMap<String,Object>();
			map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "min");
			subQuery.put("metric", "time.series.perform");
			subQuery.put("downsample", "1m-min");
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			subTag.put("dc",point.getSensorCode());
			subQuery.put("tags", subTag);	
			List<Map<String,Object>> list = new ArrayList<>(); 
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);
	}

	@Override
	public Status selectMinuteAvgByDeviceAndSensor(TsPoint point,
			Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		/*查询某个指定编号设备，指定编号传感器在某段时间,每分钟的平均值 
 比如 查询25号风机， 2017年6月3日到2017年6月4日的8号温度传感器每分钟的平均值
		 * 
		 */
		 Map<String,Object> map = new  HashMap<String,Object>();
			map.put("start",startTime.getTime());
			map.put("end", endTime.getTime());
			Map<String,Object> subQuery = new HashMap<String ,Object>();
			subQuery.put("aggregator", "avg");
			subQuery.put("metric", "time.series.perform");
			subQuery.put("downsample", "1m-avg");
			Map<String,Object> subTag = new HashMap<String ,Object>();
			subTag.put("host",point.getDeviceCode());
			subTag.put("dc",point.getSensorCode());
			subQuery.put("tags", subTag);	
			List<Map<String,Object>> list = new ArrayList<>(); 
			list.add(subQuery);
			map.put("queries",list);
			String json=JSON.toJSONString(map);
			return queriesByHttpClient(json);
	}



	@Override
	public Status updatePoints(List<TsPoint> points) {
		// TODO Auto-generated method stub
		/*
		 * 更新数据
		 * 更新数据可以通过重新赋值来操作。即在要更新的时间，时间戳重新输入相应的数据。通过insert来操作
		 * 注意：更改配置文件的默认设置tsd.storage.fix_duplicates (2.1) 将原来的false 改为true
		 * 默认并没有配置文件，需要在 opentsdb/build目录下的opentsdb.conf（如果没有则创建），在里面添加sd.storage.fix_duplicates=true
		 */
        ArrayList<PointPut> list2 = new ArrayList<PointPut>();
		if(points!=null){
			 for(TsPoint point :points) {  //points赋值
			        PointPut pointPut=new PointPut();
			        pointPut.setTimestamp(""+point.getTimestamp());
		 	        pointPut.setValue(point.getValue());
			        PointPutTag tag=new PointPutTag(point);
			        pointPut.setTags(tag);
			        list2.add(pointPut); 		        
				}
		}
		String json =JSON.toJSONString(list2);
		return insertByHttpClient(json);	
	
	}

	@Override
	public Status deletePoints(Date date) {
		// TODO Auto-generated method stub
		//这里删除好像是要改配置文件的，默认好像有什么情况是不能删除的。
		/*
		 * 删除某个时间点之前的所有数据
		 * 参考
		 * 
		 * 
		 *1、 http://opentsdb.net/docs/build/html/api_http/dropcaches.html  这个是删除内存中缓存的数据
		 * 
		 * 2、通过查询来删除数据http://opentsdb.net/docs/build/html/api_http/query/index.html#
		 * 上面连接中的第二段话，与下面的配置文件相联系
		 * 		一个相关的配置文件http://opentsdb.net/docs/build/html/user_guide/configuration.html
		 * 		中的tsd.http.query.allow_delete	Boolean	Optional	
		 * 		Whether or not to allow deleting data points from storage during query time.	False
		 * 		须将默认的false改为true
		 *   查询http://opentsdb.net/docs/build/html/api_http/query/index.html#verbs
		 *   有verb动词，delete，是不是可以和其他的一样用？只是将连接http的get、post改为delete？
		 *3、http://opentsdb.net/docs/build/html/api_http/index.html#verbs中的delete,有一个删除注释的例子
		 * 删除其他数据该如何删除？
		 *在Telnet Style API中有 dropcaches
		 *
		 *
		 *结论： 删除指定时间范围内的数据是不能通过Http来操作的。
		 */
		return null;
	}
	/**
	 * 数据生成器
	 */
	@Override
	public long generateData() {
		super.generateData();
		String path = getDataPath();
		File file = new File(path + "/" + "opentsdb_" + System.nanoTime());
		if (file.exists()) {
			file.delete();
		}
		StringBuilder sc = new StringBuilder();
		long startTime = System.nanoTime();
		long count = 0;
		try {
			FileWriter fw;
			fw = new FileWriter(file, true);
			fw.write(sc.toString());
			fw.close();
			int sumTimes = getSumTimes();// 根据总生成条数设置
			for (int i = 0; i < sumTimes; i++) {
				fw = new FileWriter(file, true);
				sc.setLength(0);
				List<TsPoint> tsFiles = Core.generateLoadData(sumTimes, i + 1);
				for (TsPoint point : tsFiles) {
					sc.append("sys.test");//TODO 该名字需要修改
					sc.append(" ");
					sc.append("device_code");
					sc.append("=");
					sc.append(point.getDeviceCode());
					sc.append(",");
					sc.append("sensor_code");
					sc.append("=");
					sc.append(point.getSensorCode());
					sc.append(" ");
					sc.append(point.getTimestamp());
					sc.append(" ");
					sc.append("value");
					sc.append("=");
					sc.append(point.getValue());
					sc.append("\r\n");
				}
				fw.write(sc.toString());
				fw.close();
				count += tsFiles.size();
			}
		} catch (IOException e) {
			System.err.println("opentsdb 数据生成异常");
			e.printStackTrace();
			System.exit(0);
		}
		long endTime = System.nanoTime();
		System.out.println("数据生成器共消耗时间[" + (endTime - startTime) / 1000 + "]s");
		System.out.println("数据生成器共生成[" + count + "]条数据");
		System.out.println("数据生成器生成速度[" + (float) count / (endTime - startTime) * 1000 + "]points/s数据");
		return count;
	}

	@Override
	public Status selectMaxByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
//		查询指定设备，指定传感器，在指定开始时间和结束时间之间的最大值
//	直接使用aggragator 
	    Map<String,Object> map = new  HashMap<String,Object>();
	 	map.put("start",startTime.getTime());
		map.put("end", endTime.getTime());
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator", "max");
		subQuery.put("metric", "time.series.perform");
		
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put("host",deviceCode);
		subTag.put("dc",sensorCode);
		subQuery.put("tags", subTag);
		
		List<Map<String,Object>> list = new ArrayList<>();
		list.add(subQuery);
		map.put("queries",list);
		String json=JSON.toJSONString(map);	
		return queriesByHttpClient(json);
	}

	@Override
	public Status selectMinByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
//		查询指定设备，指定传感器，在指定开始时间和结束时间之间的最小值
	    Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",startTime.getTime());
		map.put("end", endTime.getTime());
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator", "min");
		subQuery.put("metric", "time.series.perform");
		
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put("host",deviceCode);
		subTag.put("dc",sensorCode);
		subQuery.put("tags", subTag);
		
		List<Map<String,Object>> list = new ArrayList<>();
		list.add(subQuery);
		map.put("queries",list);
		String json=JSON.toJSONString(map);
		System.out.println(json);
		
		return queriesByHttpClient(json);
	}

	@Override
	public Status selectAvgByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
//		查询指定设备，指定传感器，在指定开始时间和结束时间之间的平均值
	    Map<String,Object> map = new  HashMap<String,Object>();
	    map.put("start",startTime.getTime());
		map.put("end", endTime.getTime());
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator", "avg");
		subQuery.put("metric", "time.series.perform");
		
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put("host",deviceCode);
		subTag.put("dc",sensorCode);
		subQuery.put("tags", subTag);
		
		List<Map<String,Object>> list = new ArrayList<>();
		list.add(subQuery);
		map.put("queries",list);
		String json=JSON.toJSONString(map);
		return queriesByHttpClient(json);
	}

	@Override
	public Status selectCountByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
//		查询指定设备，指定传感器，在指定开始时间和结束时间之间的记录数目
//		这个还只能用downsample来做http://opentsdb.net/docs/build/html/user_guide/query/aggregators.html#count
//		自己的测试query3试过可以做到
	    Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",startTime.getTime());
		map.put("end", endTime.getTime());
		long tms=endTime.getTime()-startTime.getTime();
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator", "sum");//此只能用sum，不能用count，用count则结果出错
		subQuery.put("metric", "time.series.perform");
		subQuery.put("downsample", tms+"ms-count");
		
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put("host",deviceCode);
		subTag.put("dc",sensorCode);
		subQuery.put("tags", subTag);	
		List<Map<String,Object>> list = new ArrayList<>();
		list.add(subQuery);
		map.put("queries",list);
		String json=JSON.toJSONString(map);
		return queriesByHttpClient(json);
	}


}
