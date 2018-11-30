package cn.edu.ruc.adapter;

import cn.edu.ruc.base.*;
import com.alibaba.fastjson.JSON;
import okhttp3.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class DruidAdapter implements DBAdapter{
	private String URL="http://127.0.0.1:8200";
	private static String PUT_URL="/v1/post/druidTest";
	private static String QUERY_URL="";
	MediaType MEDIA_TYPE_TEXT=MediaType.parse("text/plain");
	//MediaType MEDIA_TYPE_TEXT=MediaType.parse("application/json");
	private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
			.readTimeout(60, TimeUnit.MINUTES)
			.connectTimeout(60, TimeUnit.MINUTES)
			.writeTimeout(60, TimeUnit.MINUTES)
			.build();
	public static OkHttpClient getOkHttpClient(){
		return OK_HTTP_CLIENT;
	}





	@Override
	public void initDataSource(TsDataSource ds, TsParamConfig tspc) {
		System.out.println(ds);
		PUT_URL=URL+PUT_URL;
		QUERY_URL=URL+QUERY_URL;
	}

	@Override
	public Object preWrite(TsWrite tsWrite) {
		List<Map<String,Object>>list=new ArrayList<Map<String,Object>>();
		StringBuffer sc = new StringBuffer();
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
				pointMap.put("valueSum", tpk.getValue(sensorCode));
				//list.add(pointMap);
				sc.append(JSON.toJSONString(pointMap));
				sc.append("\n");
			}
		}
		//String json=JSON.toJSONString(list);
		//return json;
		return sc.toString();
	}

	@Override
	public Status execWrite(Object write) {
		//MEDIA_TYPE_TEXT.charset(null);
		//Request request = new Request.Builder()
		//    .url(PUT_URL)
		//    .post(RequestBody.create(MEDIA_TYPE_TEXT, write.toString()))
		//    .post(RequestBody.create(MEDIA_TYPE_TEXT, query.toString().getBytes("utf-8")))
		//    .build();
		Request request=null;
		try {
			request = new Request.Builder()
					.url(PUT_URL)
					.post(RequestBody.create(MEDIA_TYPE_TEXT, write.toString().getBytes("UTF-8")))
					.build();
		} catch (Exception e) {
			e.printStackTrace();
		}
//		System.out.println(request.body().contentType());
		return exeOkHttpRequest(request);
	}

	@Override
	public Object preQuery(TsQuery tsQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status execQuery(Object query) {
		// TODO Auto-generated method stub
		return null;
	}

	private Status exeOkHttpRequest(Request request) {
		long costTime = 0L;
		Response response;
		OkHttpClient client = getOkHttpClient();
		try {
			long startTime1=System.nanoTime();
			response = client.newCall(request).execute();
			System.out.println(response.body().string());
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
	@Override
	public void closeAdapter() {
		// TODO Auto-generated method stub

	}
	public static void main(String[] args){
		MediaType MEDIA_TYPE_TEXT=MediaType.parse("text/plain");
		Request request=null;
		try {
			request = new Request.Builder()
					.url("http://www.baidu.com")
					.post(RequestBody.create(MEDIA_TYPE_TEXT, "abc".getBytes("UTF-8")))
					.build();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println(request.body().contentType());
		System.out.println(request.body());
	}
}

