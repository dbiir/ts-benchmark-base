package cn.edu.ruc.adapter;

import cn.edu.ruc.base.*;
import com.alibaba.fastjson.JSON;
import okhttp3.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class RiakTSAdapter implements DBAdapter{

	private String URL="http://127.0.0.1:8098";
	private static String PUT_URL="/ts/v1/tables/riaktstest4/keys";
	private static String QUERY_URL="/ts/v1/query";
	MediaType MEDIA_TYPE_TEXT=MediaType.parse("text/plain");
	private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
			.readTimeout(60, TimeUnit.MINUTES)
			.connectTimeout(60, TimeUnit.MINUTES)
			.writeTimeout(60, TimeUnit.MINUTES)
			.build();
	public static OkHttpClient getOkHttpClient(){
		return OK_HTTP_CLIENT;
	}

	public void initDataSource(TsDataSource ds, TsParamConfig tspc) {
		PUT_URL=URL+PUT_URL;
		QUERY_URL=URL+QUERY_URL;
		String sql="CREATE TABLE riaktstest4 "+
				" (time timestamp not null, "+
				"device_code varchar not null, "+
				"sensor_code varchar not null, "+
				"value double,"+
				"PRIMARY KEY (("+"device_code, sensor_code, QUANTUM(time,1,'s')),"+
				"device_code, sensor_code,time"+
				"));";
		Request request = new Request.Builder()
				.url(QUERY_URL)
				.post(RequestBody.create(MEDIA_TYPE_TEXT, sql.toString()))
				.build();
		exeOkHttpRequest(request);

	}

	@Override
	public Object preWrite(TsWrite tsWrite) {
		List<Map<String,Object>>list=new ArrayList<Map<String,Object>>();
		LinkedList<TsPackage> pkgs=tsWrite.getPkgs();
		for(TsPackage tpk:pkgs) {
			String deviceCode=tpk.getDeviceCode();
			long timestamp=tpk.getTimestamp();
			Set<String> sensorCodes = tpk.getSensorCodes();
			for(String sensorCode:sensorCodes) {
				Map<String,Object> pointMap=new HashMap<>();
				pointMap.put("time", timestamp);
				pointMap.put("device_code", deviceCode);
				pointMap.put("sensor_code", sensorCode);
				pointMap.put("value",Double.parseDouble(tpk.getValue(sensorCode).toString()));
				list.add(pointMap);
			}
		}
		String json=JSON.toJSONString(list);
		//System.out.println(json);
		return json;
	}

	@Override
	public Status execWrite(Object write) {
		Request request = new Request.Builder()
				.url(PUT_URL)
				.post(RequestBody.create(MEDIA_TYPE_TEXT, write.toString()))
				.build();
		return exeOkHttpRequest(request);
	}

	@Override
	public Object preQuery(TsQuery tsQuery) {
		StringBuffer sc=new StringBuffer();
		sc.append("SELECT ");
		switch (tsQuery.getQueryType()) {
			case 1://简单查询
				sc.append(tsQuery.getSensorName());
				sc.append(" ");
				break;
			case 2://分析查询
				sc.append("");
				if(tsQuery.getQueryType()==1) {
					sc.append("max(");
				}
				if(tsQuery.getQueryType()==2) {
					sc.append("min(");
				}
				if(tsQuery.getQueryType()==3) {
					sc.append("avg(");
				}
				if(tsQuery.getQueryType()==4) {
					sc.append("count(");
				}
				sc.append(tsQuery.getSensorName());
				sc.append(") ");
				break;
			case 3://分析查询
				sc.append("");
				if(tsQuery.getAggreType()==1) {
					sc.append("max(");
				}
				if(tsQuery.getAggreType()==2) {
					sc.append("min(");
				}
				if(tsQuery.getAggreType()==3) {
					sc.append("avg(");
				}
				if(tsQuery.getAggreType()==4) {
					sc.append("count(");
				}
				sc.append(tsQuery.getSensorName());
				sc.append(") ");
				break;
			default:
				break;
		}
		sc.append(" from riaktstest where ");
		if(tsQuery.getQueryType()!=3 && !"*".equals(tsQuery.getDeviceName())){
			sc.append("device_code='");
			sc.append(tsQuery.getDeviceName());
			sc.append("' ");
			sc.append(" and ");
		}
		if(tsQuery.getQueryType()==3) {
			sc.append("device_code='");
			sc.append(tsQuery.getDeviceName());
			sc.append("' ");
			sc.append("and");
		}
		sc.append("sensor_code='");
		sc.append(tsQuery.getSensorName());
		sc.append("'");
		if(tsQuery.getStartTimestamp()!=null) {
			sc.append(" and ");
			sc.append("time >= ");
			sc.append(TimeUnit.MILLISECONDS.toNanos(tsQuery.getStartTimestamp()));
			sc.append(" ");
		}
		if(tsQuery.getEndTimestamp()!=null) {
			sc.append(" and ");
			sc.append("time <= ");
			sc.append(TimeUnit.MILLISECONDS.toNanos(tsQuery.getEndTimestamp()));
			sc.append(" ");
		}
		if(tsQuery.getSensorLtValue()!=null) {
			sc.append(" and ");
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
		/***************************************************
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
		 ****************************************/
		sc.append(";");
		return sc.toString();
	}

	@Override
	public Status execQuery(Object query) {
		Request request = new Request.Builder()
				.url(QUERY_URL)
				.post(RequestBody.create(MEDIA_TYPE_TEXT, query.toString()))
				.build();
		return exeOkHttpRequest(request);

	}
	/*************************************
	 public static void main(String[]args) {
	 DBAdapter adapter=new RiakTSAdapter2();
	 TsDataSource ds=new TsDataSource();
	 TsParamConfig tspc=new TsParamConfig();
	 adapter.initDataSource(ds, tspc);
	 TsQuery query=new TsQuery();

	 }
	 */



	private Status exeOkHttpRequest(Request request) {
		long costTime = 0L;
		Response response;
		OkHttpClient client = getOkHttpClient();
		try {
			long startTime1=System.nanoTime();
			response = client.newCall(request).execute();
			int code = response.code();
			System.out.println(response.body().string());
			response.close();
			long endTime1=System.nanoTime();
			costTime=endTime1-startTime1;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(0L);
		}
		return Status.OK(costTime);
	}
	@Override
	public void closeAdapter() {
		// TODO Auto-generated method stub

	}

}
