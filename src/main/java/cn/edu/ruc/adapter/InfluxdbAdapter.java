package cn.edu.ruc.adapter;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import cn.edu.ruc.base.Status;
import cn.edu.ruc.base.TsDataSource;
import cn.edu.ruc.base.TsPackage;
import cn.edu.ruc.base.TsParamConfig;
import cn.edu.ruc.base.TsQuery;
import cn.edu.ruc.base.TsWrite;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
/**
 * 不同的存储结构，会有不同的性能表现
 * @author fasape
 *
 */
public class InfluxdbAdapter implements DBAdapter {
	private String URL = "http://%s:%s";
	private String DB_NAME = "ruc_test_5";
	private String WRITE_URL = "/write?precision=ms&db=%s";
	private String QUERY_URL = "/query?db=%s";
	private org.influxdb.InfluxDB INFLUXDB = null;
	MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
	@Override
	public void initDataSource(TsDataSource ds,TsParamConfig tspc) {
		URL=String.format(URL,ds.getIp(),ds.getPort());
		WRITE_URL = URL +String.format(WRITE_URL,DB_NAME);
		QUERY_URL = URL +String.format(QUERY_URL, DB_NAME);
		INFLUXDB=InfluxDBFactory.connect(URL);
		INFLUXDB.setDatabase(DB_NAME);
		INFLUXDB.createDatabase(DB_NAME);
	}
	@Override
	public Object preWrite(TsWrite tsWrite) {
		LinkedList<TsPackage> pkgs = tsWrite.getPkgs();
		StringBuilder sc = new StringBuilder();
		for(TsPackage pkg:pkgs) {
			String deviceCode = pkg.getDeviceCode();
			Set<String> sensorCodes = pkg.getSensorCodes();
			for(String sensorCode:sensorCodes) {
				Object value = pkg.getValue(sensorCode);
				sc.append("sensor");
				sc.append(",");
				sc.append("device_code");
				sc.append("=");
				sc.append(deviceCode);
				sc.append(",");
				sc.append("sensor_code");
				sc.append("=");
				sc.append(sensorCode);
				sc.append(" ");
				sc.append("value");
				sc.append("=");
				sc.append(value);
				sc.append(" ");
				sc.append(pkg.getTimestamp());
				sc.append("\n");
			}
		}
		return sc.toString();
	}

	@Override
	public Status execWrite(Object writeObj) {
	    Request request = new Request.Builder()
	            .url(WRITE_URL)
	            .post(RequestBody.create(MEDIA_TYPE_TEXT, writeObj.toString()))
	            .build();
	    return exeOkHttpRequest(request);
	}

	@Override
	public Object preQuery(TsQuery tsQuery) {
		StringBuffer sc=new StringBuffer();
		sc.append("select ");
		switch (tsQuery.getQueryType()) {
		case 1://简单查询
			sc.append("time,value ");
			break;
		case 2://分析查询
			sc.append("time,");
			if(tsQuery.getAggreType()==1) {
				sc.append("max(value) ");
			}
			if(tsQuery.getAggreType()==2) {
				sc.append("min(value) ");
			}
			if(tsQuery.getAggreType()==3) {
				sc.append("mean(value) ");
			}
			if(tsQuery.getAggreType()==4) {
				sc.append("count(value) ");
			}
			break;
		case 3://分析查询
			sc.append("time,");
			if(tsQuery.getAggreType()==1) {
				sc.append("max(value) ");
			}
			if(tsQuery.getAggreType()==2) {
				sc.append("min(value) ");
			}
			if(tsQuery.getAggreType()==3) {
				sc.append("mean(value) ");
			}
			if(tsQuery.getAggreType()==4) {
				sc.append("count(value) ");
			}
			break;
		default:
			break;
		}
		sc.append("from sensor where ");
		if(tsQuery.getQueryType()!=3&&!"*".equals(tsQuery.getDeviceName())){
			sc.append("device_code='");
			sc.append(tsQuery.getDeviceName());
			sc.append("' ");
			sc.append("and ");
		}
		if(tsQuery.getQueryType()==3){
			sc.append("(");
			String template="device_code='%s'";
			List<String> devices = tsQuery.getDevices();
			for(int index=0;index<devices.size();index++){
				sc.append(String.format(template, devices.get(index)));
				if(index<devices.size()-1){
					sc.append(" or ");
				}
			}
			sc.append(")");
		}
		sc.append("sensor_code='");
		sc.append(tsQuery.getSensorName());
		sc.append("' ");
		if(tsQuery.getStartTimestamp()!=null) {
			sc.append("and ");
			sc.append("time >=");
			sc.append(TimeUnit.MILLISECONDS.toNanos(tsQuery.getStartTimestamp()));
			sc.append(" ");
		}
		if(tsQuery.getEndTimestamp()!=null) {
			sc.append("and ");
			sc.append("time <=");
			sc.append(TimeUnit.MILLISECONDS.toNanos(tsQuery.getEndTimestamp()));
			sc.append(" ");
		}
		if(tsQuery.getSensorLtValue()!=null) {
			sc.append("and ");
			sc.append("value >=");
			sc.append(tsQuery.getSensorLtValue());
			sc.append(" ");
		}
		if(tsQuery.getSensorGtValue()!=null) {
			sc.append("and ");
			sc.append("value <=");
			sc.append(tsQuery.getSensorGtValue());
			sc.append(" ");
		}
		if(tsQuery.getQueryType()==3){
			sc.append("group by device_code");
		}
		if(tsQuery.getGroupByUnit()!=null&&tsQuery.getQueryType()==2) {
			sc.append("group by ");
			switch (tsQuery.getGroupByUnit()) {
			case 1:
				sc.append(" time(1s)");
				break;
			case 2:
				sc.append(" time(1m)");
				break;
			case 3:
				sc.append(" time(1h)");
				break;
			case 4:
				sc.append(" time(1d)");
				break;
			case 5:
				sc.append(" time(1M)");
				break;
			case 6:
				sc.append(" time(1y)");
				break;
			default:
				break;
			}
		}
		return sc.toString();
	}

	@Override
	public Status execQuery(Object query) {
		long costTime = 0L;
		try {
			//TODO 需要完善
			long startTime1=System.nanoTime();
			QueryResult results = INFLUXDB.query(new Query(query.toString(), DB_NAME));
//			System.out.println(results.getResults());
			long endTime1=System.nanoTime();
			costTime=endTime1-startTime1;
			if(results.hasError()) {
				return Status.FAILED(costTime);
			}else {
				return Status.OK(costTime);
			}
		} catch (Exception e) {
			e.printStackTrace();
//			System.out.println(query);
			return Status.FAILED(0L);
		}
	}
	private Status exeOkHttpRequest(Request request) {
		long costTime = 0L;
	    Response response;
	    OkHttpClient client = getOkHttpClient();
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
	public static void main(String[] args) {
		TsQuery query=new TsQuery();
		query.setAggreType(2);
		query.setDeviceName("d2");
		query.setSensorName("s1");
		query.setSensorLtValue(54.0);
		query.setSensorGtValue(12.0);
		query.setAggreType(2);
		query.setGroupByUnit(2);
		query.setQueryType(1);
		DBAdapter adapter=new InfluxdbAdapter();
		System.out.println(adapter.preQuery(query));
	}
	@Override
	public void closeAdapter() {
		// TODO Auto-generated method stub
		
	}
	private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
		       .readTimeout(500000, TimeUnit.MILLISECONDS)
		       .connectTimeout(500000, TimeUnit.MILLISECONDS)
		       .writeTimeout(500000, TimeUnit.MILLISECONDS)
		       .build();
	public static OkHttpClient getOkHttpClient(){
		return OK_HTTP_CLIENT;
	}
}
