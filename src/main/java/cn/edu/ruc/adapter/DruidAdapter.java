package cn.edu.ruc.adapter;

import cn.edu.ruc.base.*;
import com.alibaba.fastjson.JSON;
import okhttp3.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class DruidAdapter implements DBAdapter{
	private String URL1="http://127.0.0.1:8200";
	private String URL2="http://127.0.0.1:8082";
	private static String PUT_URL="/v1/post/druidTest";
	private static String QUERY_URL="/druid/v2?pretty";
	MediaType MEDIA_TYPE_TEXT=MediaType.parse("application/json");
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
		PUT_URL=URL1+PUT_URL;
		QUERY_URL=URL2+QUERY_URL;
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
		Map<String,Object> map=new HashMap<String,Object>();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		Long unix_start=tsQuery.getStartTimestamp();
		Long unix_end=tsQuery.getEndTimestamp();
		String start = format.format(unix_start);
		String end=format.format(unix_end);
		String array_time[]=new String[1];
		array_time[0]=start+"/"+end;

		/*
		 * select查询
		 * {
             "queryType": "select",
             "dataSource": "druidTest",
             "dimensions":["device_code","sensor_code"],
             "metrics":[],
             "filter":{
             "type":"and",
             "fields":[
             {"type": "selector", "dimension": "device_code", "value": getDeviceCode},
             {"type": "selector", "dimension": "sensor_code", "value": getSensorCode}
             ]
             }
             "granularity": "all",
             "intervals": ["start/end"],
             "pagingSpec":{"pagingIdentifiers": {}, "threshold":5}
            }
		 */


		if(tsQuery.getQueryType()==1)
		{
			map.put("queryType", "select");
			map.put("dataSource", "druidTest");
			Object array_dimensions[]=new Object[1];
			map.put("dimensions",new ArrayList<Object>());
			Object a[]=new Object[1];
			map.put("metrics",new ArrayList<Object>());
			Object fields[]=new Object[2];
			Map<String,Object> json1=new HashMap<String,Object>();
			Map<String,Object> json2=new HashMap<String,Object>();
			json1.put("type", "selector");
			json2.put("type", "selector");
			json1.put("dimension","device_code");
			json2.put("dimension","sensor_code");
			json1.put("value", tsQuery.getDeviceName());
			json2.put("value", tsQuery.getSensorName());
			fields[0]=json1;
			fields[1]=json2;
			Map<String,Object> filter=new HashMap<String,Object>();
			filter.put("type","and");
			filter.put("fields",fields);
			map.put("filter", filter);
			map.put("granularity", "all");
			map.put("intervals", array_time);
			Map<String,Object> pagingSpec=new HashMap<String,Object>();
			Map<String,Object> null_page=new HashMap<String,Object>();
			pagingSpec.put("pagingIdentifiers", null_page);
			pagingSpec.put("threshold", 5);
			map.put("pagingSpec", pagingSpec);

		}
		/*
		 * {
  "queryType": "groupBy",
  "dataSource": "druidTest",
  "granularity": "",
  "dimensions": ["device_code","sensor_code"],

  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "device_code", "value": "getDeviceName" },
      { "type": "selector", "dimension": "sensor_code", "value": "getSensorName" }
    ]
  },
  "aggregations": [
    { "type": "doubleMax or doubleMin or doubleAvg or count ", "name": "total_usage", "fieldName": "user_count" }
  ],
  "postAggregations": [
    { "type": "arithmetic",
      "name": "avg_usage",
      "fn": "/",
      "fields": [
        { "type": "fieldAccess", "fieldName": "sum" },
        { "type": "fieldAccess", "fieldName": "count" }
      ]
    }
  ],
  "intervals": [ "2012-01-01T00:00:00.000/2012-01-03T00:00:00.000" ]
}
		 */
		if(tsQuery.getQueryType()==2)
		{
			map.put("queryType", "groupBy");
			map.put("dataSource", "druidTest");
			if(tsQuery.getGroupByUnit()!=null) {
				switch (tsQuery.getGroupByUnit()) {
					case 1:
						map.put("granularity", "second");
						break;
					case 2:
						map.put("granularity", "minute");
						break;
					case 3:
						map.put("granularity", "hour");
						break;
					case 4:
						map.put("granularity", "day");
						break;
					case 5:
						map.put("granularity", "month");
						break;
					case 6:
						map.put("granularity", "year");
						break;
					default:
						break;
				}
			}else {
				map.put("granularity", "all");
			}

			//select dimensions
			String array_dimensions[]=new String[2];
			array_dimensions[0]="device_code";
			array_dimensions[1]="sensor_code";
			map.put("dimensions",array_dimensions);
			Object fields[]=new Object[2];
			Map<String,Object> json1=new HashMap<String,Object>();
			Map<String,Object> json2=new HashMap<String,Object>();
			json1.put("type", "selector");
			json2.put("type", "selector");
			json1.put("dimension","device_code");
			json2.put("dimension","sensor_code");
			json1.put("value", tsQuery.getDeviceName());
			json2.put("value", tsQuery.getSensorName());
			fields[0]=json1;
			fields[1]=json2;
			Map<String,Object> filter=new HashMap<String,Object>();
			filter.put("type","and");
			filter.put("fields",fields);
			map.put("filter", filter);

			//aggregations
			Object aggregations[]=new Object[3];
			Object aggregations_avg[]=new Object[2];

			Map<String,Object> count=new HashMap<String,Object>();
			Map<String,Object> doubleSum=new HashMap<String,Object>();
			Map<String,Object> doubleX=new HashMap<String,Object>();
			count.put("type" , "count");
			count.put("name" , "count");
			count.put("fieldName","valueSum");
			doubleSum.put("type", "doubleSum");
			doubleSum.put("name", "sum");
			doubleSum.put("fieldName", "valueSum");
			if(tsQuery.getAggreType()==1)
			{
				doubleX.put("type","doubleMax");
				doubleX.put("name","Max");
				doubleX.put("fieldName","valueSum");
				map.put("aggregations",doubleX);
			}
			if(tsQuery.getAggreType()==2)
			{
				doubleX.put("type","doubleMin");
				doubleX.put("name","Min");
				doubleX.put("fieldName","valueSum");
				map.put("aggregations",doubleX);
			}
			aggregations_avg[0]=count;
			aggregations_avg[1]=doubleSum;
			//aggregations[2]=doubleX;
			map.put("aggregations",aggregations_avg);
			//postAggregations
			if(tsQuery.getAggreType()==3)
			{
				Map<String,Object> postAggregations=new HashMap<String,Object>();
				Object fields2[]=new Object[2];
				Map<String,Object> json3=new HashMap<String,Object>();
				Map<String,Object> json4=new HashMap<String,Object>();
				json3.put("type", "fieldAccess");
				json4.put("type", "fieldAccess");
				json3.put("fieldName","sum");
				json4.put("fieldName","count");
				fields2[0]=json3;
				fields2[1]=json4;
				postAggregations.put("type","arithmetic");
				postAggregations.put("name","avg");
				postAggregations.put("fn", "/");
				postAggregations.put("fields", fields2);
				Object a[]=new Object[1];
				a[0]=postAggregations;
				map.put("postAggregations", a);
			}
		}
		map.put("intervals", array_time);
		String json=JSON.toJSONString(map);
		return json;

	}

	@Override
	public Status execQuery(Object query) {


//		System.out.print(query);
		Request request=null;
		try {
			request = new Request.Builder()
					.url(QUERY_URL)
					.post(RequestBody.create(MEDIA_TYPE_TEXT, query.toString().getBytes("UTF-8")))
					.build();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(request.body().contentType());
		return exeOkHttpRequest(request);
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
		DruidAdapter adapter=new DruidAdapter();
		TsQuery query=new TsQuery();
		query.setQueryType(2);
		query.setDeviceName("d_1");
		query.setSensorName("s_49");
		query.setStartTimestamp(1514736000000L);
		query.setEndTimestamp(1514822400000L);
		query.setQueryType(1);
		query.setAggreType(3);
		query.setGroupByUnit(2);
		System.out.println(adapter.preQuery(query));
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

