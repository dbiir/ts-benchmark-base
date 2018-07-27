package cn.edu.ruc.adapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;

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

public class OpentsdbAdapter implements DBAdapter {
	private static String URL="http://%s:%s";
	private static String PUT_URL="/api/put";
	private static String QUERY_URL="/api/query";
	private static  String METRIC="wind.perform";//time.series.perform
	private static String DEViCE_TAG="d";
	private static String SENSOR_TAG="s";
	MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
	private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
		       .readTimeout(500000, TimeUnit.MILLISECONDS)
		       .connectTimeout(500000, TimeUnit.MILLISECONDS)
		       .writeTimeout(500000, TimeUnit.MILLISECONDS)
		       .build();
	public static OkHttpClient getOkHttpClient(){
		return OK_HTTP_CLIENT;
	}
	@Override
	public void initDataSource(TsDataSource ds,TsParamConfig tspc) {
	    	URL = String.format(URL,ds.getIp(),ds.getPort());
	    	PUT_URL=URL+PUT_URL;
	    	QUERY_URL=URL+QUERY_URL;
	}

	@Override
	public Object preWrite(TsWrite tsWrite) {
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		LinkedList<TsPackage> pkgs = tsWrite.getPkgs();
		for(TsPackage tpk:pkgs) {
			String deviceCode = tpk.getDeviceCode();
			long timestamp = tpk.getTimestamp();
			Set<String> sensorCodes = tpk.getSensorCodes();
			for(String sensorCode:sensorCodes) {
				Map<String,Object> pointMap=new HashMap<>();
				Map<String,Object> mapTags=new HashMap<>();
				mapTags.put(DEViCE_TAG,deviceCode);
				mapTags.put(SENSOR_TAG, sensorCode);
				pointMap.put("metric", METRIC);
				pointMap.put("timestamp", timestamp);
				pointMap.put("value",tpk.getValue(sensorCode));
				pointMap.put("tags", mapTags);
				list.add(pointMap);
			}
		}
		String json =JSON.toJSONString(list);
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
		Map<String,Object> map = new  HashMap<String,Object>();
		map.put("start",tsQuery.getStartTimestamp());
		map.put("end", tsQuery.getEndTimestamp());
		Map<String,Object> subQuery = new HashMap<String ,Object>();
		subQuery.put("aggregator", "avg");
		subQuery.put("metric", METRIC);
		Map<String,Object> subTag = new HashMap<String ,Object>();
		subTag.put(DEViCE_TAG,tsQuery.getDeviceName());
		subTag.put(SENSOR_TAG,tsQuery.getSensorName());
		if(tsQuery.getQueryType()==2) {
			String aggerType="max";
			String timeUnit="h";
			String interval="1";
			switch (tsQuery.getAggreType()) {
			case 1:
				aggerType="max";
				break;
			case 2:
				aggerType="min";
				break;
			case 3:
				aggerType="avg";
				break;
			case 4:
				aggerType="count";
				break;
			default:
				break;
			}
			//1:s 2:min 3:hour 4:day 5:month 6:year
			if(tsQuery.getGroupByUnit()!=null) {
				switch (tsQuery.getGroupByUnit()) {
				case 1:
					timeUnit="s";
					break;
				case 2:
					timeUnit="m";
					break;
				case 3:
					timeUnit="h";
					break;
				case 4:
					timeUnit="d";
					break;
				case 5:
					timeUnit="M";
					break;
				case 6:
					timeUnit="y";
					break;
				default:
					break;
				}
			}else {
				interval=tsQuery.getEndTimestamp()-tsQuery.getEndTimestamp()+"";
				timeUnit="ms";
			}
			subQuery.put("downsample", String.format("%s%s-%s", interval,timeUnit,aggerType));
		}
		subQuery.put("tags", subTag);
		List<Map<String,Object>> list = new ArrayList<>(); 
		list.add(subQuery);
		map.put("queries",list);
		String json=JSON.toJSONString(map);
		return json;
	}

	@Override
	public Status execQuery(Object query) {
	    Request request = new Request.Builder()
	            .url(QUERY_URL)
	            .post(RequestBody.create(MEDIA_TYPE_TEXT, query.toString()))
	            .build();
		return exeOkHttpRequest(request);
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
	@Override
	public void closeAdapter() {
		// TODO Auto-generated method stub
		
	}
}
