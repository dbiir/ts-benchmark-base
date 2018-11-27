package cn.edu.ruc.utils;

import com.alibaba.fastjson.JSON;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;


public class GenerateData {
	private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    public static void main(String[] args) throws Exception {
    	String startDateStr="2018-01-01T00:00:00.000";
    	String endDateStr="2018-01-08T00:00:00.000";
		long startTime = DATE_FORMAT.parse(startDateStr).getTime();
		long currentTime= startTime;
		long endTime=DATE_FORMAT.parse(endDateStr).getTime();
		
		Random r =new Random();
		System.out.println(startTime);
		System.out.println(endTime);
		while(currentTime<endTime){
			StringBuffer sc=new StringBuffer();
			for(int dn=0;dn<100;dn++){
				for(int sn=0;sn<150;sn++){
					Map<String,Object> map=new TreeMap<String,Object>();
					String dateFormat = DATE_FORMAT.format(new Date(currentTime));
					Object value=0;
					if(r.nextDouble()<1){
					    value= String.format("%.2f",r.nextFloat()*1000);
                    }else{
					    value= r.nextInt(100);
                    }
					map.put("time", dateFormat);
					map.put("device_code", "d_"+dn);
					map.put("sensor_code", "s_"+sn);
					map.put("valueSum",value);
					sc.append(JSON.toJSON(map));
					sc.append("\n");
//					System.out.println(JSON.toJSON(map));
				}
			}
			appendFile(sc.toString(),"E:\\ac.txt");
			currentTime+=7000;
		}
		String str="";
		
//		Date date = new Date(startTime);
//		String format = DATE_FORMAT.format(date);
//		System.out.println(format);
	}
	private static void appendFile(String data,String path) {
		FileWriter fw = null;
		try {
		//如果文件存在，则追加内容；如果文件不存在，则创建文件
//			File f=new File("E:\\dd.txt");
			File f=new File(path);
			fw = new FileWriter(f, true);
		} catch (Exception e) {
		e.printStackTrace();
		}
		PrintWriter pw = new PrintWriter(fw);
		pw.println(data);
		pw.flush();
		try {
			fw.flush();
			pw.close();
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
