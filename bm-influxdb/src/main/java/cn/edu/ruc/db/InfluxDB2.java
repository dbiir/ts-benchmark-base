package cn.edu.ruc.db;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import cn.edu.ruc.biz.Core;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * influxDB处理数据库类
 * 该类使用influxdb 所提供的http-api
 * 
 * @author RUC
 */
public class InfluxDB2 extends DBBase {
	private static final String DB_URL_INFLUXDB_PROPERTY = "influxdb.url";
	private static final String DB_NAME_PROPERTY = "influxdb.database";
	private String URL = "";
	private String DB_NAME = "influxdb";
	private String WRITE_URL = "/write?precision=ms&db=";
	private String QUERY_URL = "/query?db=";
	private org.influxdb.InfluxDB INFLUXDB = null;
	MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
	public static void main(String[] args) throws Exception {
		Core.main(args);
	}

	@Override
	public String getDBUrl() {
		return URL;
	}

	@Override
	public void init() {
		super.init();
		Properties prop = getProperties();
		URL = prop.getProperty(DB_URL_INFLUXDB_PROPERTY, "http://127.0.0.1:8086");
		DB_NAME = prop.getProperty(DB_NAME_PROPERTY, "influxdb");
		WRITE_URL = URL + WRITE_URL + DB_NAME;
		QUERY_URL = URL + QUERY_URL + DB_NAME;
		createTestDB();
		INFLUXDB=InfluxDBFactory.connect(URL);
		INFLUXDB.setDatabase(DB_NAME);
	}

	@Override
	public long generateData() {
		String path = getDataPath();
		File file = new File(path + "/" + DB_NAME + System.currentTimeMillis());
		if (file.exists()) {
			file.delete();
		}
		StringBuilder sc = new StringBuilder();
		sc.append("# DDL");
		sc.append("\r\n");
		sc.append("CREATE DATABASE ");
		sc.append(DB_NAME);
		sc.append("\r\n");
		sc.append("# DML");
		sc.append("\r\n");
		sc.append("# CONTEXT-DATABASE: ");
		sc.append(DB_NAME);
		sc.append("\r\n\r\n\r\n\r\n\r\n");
		long startTime = System.currentTimeMillis();
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
					sc.append("point");
					sc.append(",");
					sc.append("device_code");
					sc.append("=");
					sc.append(point.getDeviceCode());
					sc.append(",");
					sc.append("sensor_code");
					sc.append("=");
					sc.append(point.getSensorCode());
					sc.append(" ");
					sc.append("value");
					sc.append("=");
					sc.append(point.getValue());
					sc.append(" ");
					sc.append(point.getTimestamp());
					sc.append("\n");
				}
				fw.write(sc.toString());
				fw.close();
				count += tsFiles.size();
			}
		} catch (IOException e) {
			System.err.println("influxdb 数据生成异常");
			e.printStackTrace();
			System.exit(0);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("数据生成器共消耗时间[" + (endTime - startTime) / 1000 + "]s");
		System.out.println("数据生成器共生成[" + count + "]条数据");
		System.out.println("数据生成器生成速度[" + (float) count / (endTime - startTime) * 1000 + "]points/s数据");
		return count;
	}

	private void createTestDB() {
		HttpClient hc = getHttpClient();
		HttpPost post = new HttpPost(QUERY_URL);
		HttpResponse response = null;
		try {
			List<NameValuePair> nameValues = new ArrayList<NameValuePair>();
			String createSql = "CREATE DATABASE " + DB_NAME;
			NameValuePair nameValue = new BasicNameValuePair("q", createSql);
			nameValues.add(nameValue);
			HttpEntity entity = new UrlEncodedFormEntity(nameValues, "utf-8");
			post.setEntity(entity);
			response = hc.execute(post);
			closeHttpClient(hc);
//			System.out.println(response);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			closeResponse(response);
			closeHttpClient(hc);
		}
	}

	@Override
	public Status insertMulti(List<TsPoint> points) {
		StringBuilder sc = new StringBuilder();
		if (points != null) {
			for (TsPoint point : points) {
				if(point==null){
					continue;
				}
				sc.append("sensor");
				sc.append(",");
				sc.append("device_code");
				sc.append("=");
				sc.append(point.getDeviceCode());
				sc.append(",");
				sc.append("sensor_code");
				sc.append("=");
				sc.append(point.getSensorCode());
				sc.append(" ");
				sc.append("value");
				sc.append("=");
				sc.append(point.getValue());
				sc.append(" ");
				sc.append(point.getTimestamp());
				sc.append("\n");
			}
		}
//		return insertByHttpClient(sc.toString());
		return insertByOkHttp(sc.toString());
	}
	private Status insertByOkHttp(String points) {
	    Request request = new Request.Builder()
	            .url(WRITE_URL)
	            .post(RequestBody.create(MEDIA_TYPE_TEXT, points))
	            .build();
		return exeOkHttpRequest(request);
	}

	//FIXME insert influxdb会主动关闭连接，而query不会
	private Status insertByHttpClient(String data) {
		HttpClient hc = getHttpClient();
		HttpPost post = new HttpPost(WRITE_URL);
		HttpResponse response = null;
		long costTime = 0L;
		try {
			HttpEntity entity = new StringEntity(data);
			post.setEntity(entity);
//			post.addHeader("Connection", "close");//TODO  不带close资源耗费挺少的
			long startTime = System.nanoTime();
			response = hc.execute(post);
			long endTime = System.nanoTime();
			costTime = endTime - startTime;
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode >= 200 && statusCode < 300) {
				return Status.OK(costTime);
			} else {
				System.out.println(EntityUtils.toString(response.getEntity()));
				System.out.println(statusCode+":"+costTime);
				return Status.FAILED(costTime);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(-1);
		}finally{
			closeResponse(response);
			closeHttpClient(hc);
		}
		
	}

	private HttpClient getHttpClient() {
		return HttpPoolManager.getHttpClient();
	}
	/**
	 * 关闭httpClient连接  
	 * 可优化
	 * @param hc
	 * @throws Exception
	 */
	private void closeHttpClient(HttpClient hc) {
//		if(hc instanceof Closeable){
//			try {
//				hc.getConnectionManager().closeIdleConnections(0,TimeUnit.SECONDS);
//				((Closeable)hc).close();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
	}
	/**
	 * 关闭response
	 * @param response
	 */
	private void closeResponse(HttpResponse response) {
		if(response!=null){
            try {
            	HttpEntity entity = response.getEntity();
            	if(entity!=null){
            		InputStream in = entity.getContent();
            		if(in!=null){
            			in.close();
            		}
            	}
			} catch (IOException e1) {
				e1.printStackTrace();
			}
//			if(response instanceof Closeable){
//				try {
//					((Closeable)response).close();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
		}
	}
	@Override
	public Status selectByDevice(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT * FROM sensor where device_code='" + point.getDeviceCode() + "' and time>="
				+ TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime());
		return execInfluxdbQuery(selectSql);	
	}


	@Override
	public Status selectDayMaxByDevice(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT MAX(value) FROM sensor where device_code='" + point.getDeviceCode()
		+ "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime()) + " group by time(1d)";
		return execInfluxdbQuery(selectSql);	
	}

	@Override
	public Status selectDayMinByDevice(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT MIN(value) FROM sensor where device_code='" + point.getDeviceCode()
		+ "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime()) + " group by time(1d)";
		return execInfluxdbQuery(selectSql);	
	}

	@Override
	public Status selectDayAvgByDevice(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT MEAN(value) FROM sensor where device_code='" + point.getDeviceCode()
		+ "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime()) + " group by time(1d)";
		return execInfluxdbQuery(selectSql);	
	}

	@Override
	public Status selectHourMaxByDevice(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT MAX(value) FROM sensor where device_code='" + point.getDeviceCode()
		+ "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime()) + " group by time(1h)";
		return execInfluxdbQuery(selectSql);	
	}

	@Override
	public Status selectHourMinByDevice(TsPoint point, Date startTime, Date endTime) {	
		String selectSql = "SELECT MIN(value) FROM sensor where device_code='" + point.getDeviceCode()
		+ "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime()) + " group by time(1h)";
		return execInfluxdbQuery(selectSql);	
	}

	@Override
	public Status selectHourAvgByDevice(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT MEAN(value) FROM sensor where device_code='" + point.getDeviceCode()
				+ "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime()) + " group by time(1h)";
		return execInfluxdbQuery(selectSql);
	}

	@Override
	public Status selectMinuteMaxByDeviceAndSensor(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT MAX(value) FROM sensor where device_code='" + point.getDeviceCode()
		+ "' and sensor_code='" + point.getSensorCode() + "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime())
		+ " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime()) + " group by time(1m)";
		return execInfluxdbQuery(selectSql);
	}

	@Override
	public Status selectMinuteMinByDeviceAndSensor(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT MIN(value) FROM sensor where device_code='" + point.getDeviceCode()
		+ "' and sensor_code='" + point.getSensorCode() + "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime())
		+ " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime()) + " group by time(1m)";
		return execInfluxdbQuery(selectSql);
	}

	@Override
	public Status selectMinuteAvgByDeviceAndSensor(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT MEAN(value) FROM sensor where device_code='" + point.getDeviceCode()
		+ "' and sensor_code='" + point.getSensorCode() + "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime())
		+ " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime()) + " group by time(1m)";
		return execInfluxdbQuery(selectSql);
	}

	@Override
	public Status updatePoints(List<TsPoint> points) {
		StringBuilder sc = new StringBuilder();
		if (points != null) {
			for (TsPoint point : points) {
				sc.append("sensor");
				sc.append(",");
				sc.append("device_code");
				sc.append("=");
				sc.append(point.getDeviceCode());
				sc.append(",");
				sc.append("sensor_code");
				sc.append("=");
				sc.append(point.getSensorCode());
				sc.append(" ");
				sc.append("value");
				sc.append("=");
				sc.append(point.getValue());
				sc.append(" ");
				sc.append(point.getTimestamp());
				sc.append("\n");
			}
		}
		return insertByHttpClient(sc.toString());
	}

	@Override
	public Status deletePoints(Date date) {
		String deleteSql = "DELETE  FROM sensor where time<" +TimeUnit.MILLISECONDS.toNanos(date.getTime());
		FormBody body=new FormBody.Builder().add("q", deleteSql).build();
	    Request request = new Request.Builder()
        .url(QUERY_URL)
        .post(body)
        .build();
		return exeOkHttpRequest(request);
	}

	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Date startTime, Date endTime) {
		String selectSql = "SELECT * FROM sensor where device_code='" + point.getDeviceCode()
				+ "' and sensor_code='" + point.getSensorCode() + "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime())
				+ " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime());
		return execInfluxdbQuery(selectSql);
	}

	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Double max, Double min, Date startTime, Date endTime) {
		String selectSql = "SELECT * FROM sensor where device_code='" + point.getDeviceCode()
		+ "' and sensor_code='" + point.getSensorCode() + "' and value<" + max + " and value>" + min
		+ " and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime());
		return execInfluxdbQuery(selectSql);
	}

	@Override
	public Status selectMaxByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		String selectSql = "SELECT MAX(value) FROM sensor where device_code='" + deviceCode + "' and sensor_code='"
				+ sensorCode + "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime());
		return execInfluxdbQuery(selectSql);
	}
	private Status execInfluxdbQuery(String sql) {
		long costTime = 0L;
		try {
			//TODO 需要完善
			long startTime1=System.nanoTime();
			QueryResult results = INFLUXDB.query(new Query(sql, DB_NAME));
			long endTime1=System.nanoTime();
			costTime=endTime1-startTime1;
			if(results.hasError()) {
				return Status.FAILED(costTime);
			}else {
				return Status.OK(costTime);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(0L);
		}
	}
	@Override
	public Status selectMinByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		String selectSql = "SELECT MIN(value) FROM sensor where device_code='" + deviceCode + "' and sensor_code='"
				+ sensorCode + "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime());
		return execInfluxdbQuery(selectSql);
	}

	@Override
	public Status selectAvgByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		String selectSql = "SELECT MEAN(value) FROM sensor where device_code='" + deviceCode + "' and sensor_code='"
				+ sensorCode + "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime());
		return execInfluxdbQuery(selectSql);
	}

	@Override
	public Status selectCountByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		String selectSql = "SELECT COUNT(*) FROM sensor where device_code='" + deviceCode + "' and sensor_code='"
				+ sensorCode + "' and time>=" + TimeUnit.MILLISECONDS.toNanos(startTime.getTime()) + " and time<=" + TimeUnit.MILLISECONDS.toNanos(endTime.getTime());
		return execInfluxdbQuery(selectSql);
	};
	private Status exeOkHttpRequest(Request request) {
		long costTime = 0L;
	    Response response;
	    OkHttpClient client = HttpPoolManager.getOkHttpClient();
		try {
			long startTime1=System.nanoTime();
			response = client.newCall(request).execute();
			int code = response.code();
			response.close();
			long endTime1=System.nanoTime();
			costTime=endTime1-startTime1;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(0L);
		}
		return Status.OK(costTime);
	}
}
