package cn.edu.ruc.adapter;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import cn.edu.ruc.base.Status;
import cn.edu.ruc.base.TsDataSource;
import cn.edu.ruc.base.TsPackage;
import cn.edu.ruc.base.TsParamConfig;
import cn.edu.ruc.base.TsQuery;
import cn.edu.ruc.base.TsWrite;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class CTsdbAdapter implements DBAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(CTsdbAdapter.class);
    private String URL="http://%s:%s";
    private String queryUrl;
    private String writeUrl;
    private String metricUrl;
    private String metric = "root.perform";
    private String dataType = "double";
    private Random sensorRandom = null;
    private static final String user = "root";
    private static final String pwd = "Root_1230!";
    private static final String TIME_FORMAT = "epoch_millis";
//    private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
//    private SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
    MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
	private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
		       .readTimeout(60, TimeUnit.MINUTES)
		       .connectTimeout(60, TimeUnit.MINUTES)
		       .writeTimeout(60, TimeUnit.MINUTES)
		       .build();
	public static OkHttpClient getOkHttpClient(){
		return OK_HTTP_CLIENT;
	}
    static class MyAuthenticator extends Authenticator{
        public PasswordAuthentication getPasswordAuthentication() {
            System.err.println("Feeding username and password for " + getRequestingScheme());
            return (new PasswordAuthentication(user, pwd.toCharArray()));
        }
    }


    public static void main(String[] arg) throws SQLException{
    }

	@Override
	public void initDataSource(TsDataSource ds, TsParamConfig tspc) {
		Authenticator.setDefault(new MyAuthenticator());
		URL=String.format(URL,ds.getIp(),ds.getPort());
        queryUrl = URL + "/%s/_search";
        metricUrl = URL + "/_metric/";
        
        //create new metric
        String url = metricUrl + metric ;
        int dNum=tspc.getDeviceNum()*100;
        int sNum=tspc.getSensorNum();
        CTSDBMetricModel ctsdbMetricModel = new CTSDBMetricModel();
        Map<String, String> tags = new HashMap<>();
        for(int i=0;i<dNum;i++) {
	        	tags.put("device", "d_"+i);
        }
        Map<String, String> fields = new HashMap<>();
        for(int i=0;i<sNum;i++) {
        		fields.put("s_"+i, dataType);
        }
        ctsdbMetricModel.setTags(tags);
        ctsdbMetricModel.setFields(fields);
        String body = JSON.toJSONString(ctsdbMetricModel);
    	    Request request = new Request.Builder()
    	    		.header("Authorization", Credentials.basic(user, pwd))
    	            .url(url)
    	            .post(RequestBody.create(MEDIA_TYPE_TEXT, body))
    	            .build();
    		exeOkHttpRequest(request);
	}

	@Override
	public Object preWrite(TsWrite tsWrite) {
		LinkedList<TsPackage> pkgs = tsWrite.getPkgs();
		StringBuilder body = new StringBuilder();
		for(TsPackage pkg:pkgs) {
			String deviceCode = pkg.getDeviceCode();
			body.append(getMetaJSON(deviceCode)).append("\n");
			body.append(getDataJSON(pkg)).append("\n");
		}
		return body.toString();
	}

	@Override
	public Status execWrite(Object write) {
		writeUrl = URL + "/" + metric + "/doc/_bulk";
	    Request request = new Request.Builder()
	    		.header("Authorization", Credentials.basic(user, pwd))
	            .url(writeUrl)
	            .post(RequestBody.create(MEDIA_TYPE_TEXT, write.toString()))
	            .build();
		return exeOkHttpRequest(request);
	}

	@Override
	public Object preQuery(TsQuery tsQuery) {
        long sTime = tsQuery.getStartTimestamp();
        long eTime = tsQuery.getEndTimestamp();
        List<String> sensorList = new ArrayList<>();
        sensorList.add(tsQuery.getSensorName());
        Collections.shuffle(sensorList, sensorRandom);

        Map<String, Object> queryMap = new HashMap<>();
        Map<String, Object> subQueryMap = new HashMap<>();
        Map<String, Object> boolMap = new HashMap<>();
        List<Map<String, Object>> filterList = new ArrayList<>();
        Map<String, Object> rangeMap = new HashMap<>();
        Map<String, Object> supRangeMap = new HashMap<>();
        Map<String, Object> timestampMap = new HashMap<>();
        Map<String, Object> termsMap = new HashMap<>();
        Map<String, Object> supTermsMap = new HashMap<>();
        List<String> deviceList = new ArrayList<>();
        List<String> docValueFieldsList = new ArrayList<>();

        timestampMap.put("format", TIME_FORMAT);
        timestampMap.put("gt", sTime);
        timestampMap.put("lt", eTime);
        timestampMap.put("time_zone", "+08:00");
        rangeMap.put("timestamp", timestampMap);
        supRangeMap.put("range", rangeMap);
        filterList.add(supRangeMap);
        	deviceList.add(tsQuery.getDeviceName());
        termsMap.put("device", deviceList);
        supTermsMap.put("terms", termsMap);
        filterList.add(supTermsMap);


        boolMap.put("filter", filterList);
        subQueryMap.put("bool", boolMap);
        queryMap.put("query", subQueryMap);

        for(int i = 0;i < sensorList.size();i++){
            docValueFieldsList.add(sensorList.get(i));
        }
        docValueFieldsList.add("timestamp");
        queryMap.put("docvalue_fields", docValueFieldsList);

        switch (tsQuery.getQueryType()) {
            case 1:// 简单查询
                break;
            case 2:// 分析查询
	            	Map<String, String> fieldMap = new HashMap<>();
	            Map<String, Object> aggFunctionMap = new HashMap<>();
	            Map<String, Object> resultNameMap = new HashMap<>();
	            //not support multiple fields aggregation
	            fieldMap.put("field", sensorList.get(0));
	            String aggerType="max";
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
					aggerType="value_count";
					break;
				default:
					break;
				}
	            aggFunctionMap.put(aggerType,fieldMap);
	            String resultName = aggerType+ "_" + sensorList.get(0);
	            resultNameMap.put(resultName, aggFunctionMap);
	            queryMap.put("aggs", resultNameMap);
	            break;
            case 3:// 多设备分析查询
                break;
        }
        return JSON.toJSONString(queryMap);
	}

	@Override
	public Status execQuery(Object query) {
	    Request request = new Request.Builder()
	    		.header("Authorization", Credentials.basic(user, pwd))
	            .url(queryUrl)
	            .post(RequestBody.create(MEDIA_TYPE_TEXT, query.toString()))
	            .build();
		return exeOkHttpRequest(request);
	}

	@Override
	public void closeAdapter() {
		// TODO Auto-generated method stub
		
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
	private String getDataJSON(TsPackage pkg) {
		Map<String,Object> dataMap=new TreeMap<String,Object>();
		dataMap.put("device",pkg.getDeviceCode());
		for(String sensor:pkg.getSensorCodes()) {
			dataMap.put(sensor, pkg.getValue(sensor));
		}
		dataMap.put("timestamp", pkg.getTimestamp());
		return JSON.toJSONString(dataMap);
	}
    private String getMetaJSON(String device) {
        return "{\"index\":{\"_routing\":\"" + device + "\"}}";
    }
}
