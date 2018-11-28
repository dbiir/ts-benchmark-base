package cn.edu.ruc.utils;

import com.alibaba.fastjson.JSON;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;


public class GenerateData {
	private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	private static double INTERNAL=7;
    public static void main(String[] args) throws Exception {
        INTERNAL=Double.parseDouble(args[0]);
		String path=String.format("druid_data_%d.%.4f.txt",System.currentTimeMillis(),INTERNAL);
        String startDateStr="2018-01-01T00:00:00.000";
        long startTime = DATE_FORMAT.parse(startDateStr).getTime();
//        String endDateStr="2018-01-08T00:00:00.000";
		long currentTime= startTime;
//		long endTime=DATE_FORMAT.parse(endDateStr).getTime();
		long endTime=startTime+(long)(INTERNAL*24L*3600L*1000L);
		Random r =new Random();
		StringBuffer sc=new StringBuffer();
		while(currentTime<endTime){
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
			if((currentTime-startTime)%700000==0){
				appendFile(sc.toString(),path);
				sc.setLength(0);
				System.out.println(String.format(">>%s\tfinished",DATE_FORMAT.format(new Date(currentTime))));
			}
			currentTime+=7000;
		}
		appendFile(sc.toString(),path);
	}
	private static void appendFile(String data,String path) {
		FileWriter fw = null;
		try {
			File f=new File("druid_data/"+path);
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
