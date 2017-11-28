package cn.edu.ruc.db;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.github.kevinsawicki.http.HttpRequest;

public class HttpRequestTest {
	public static void main(String[] args) {
		ExecutorService pool = Executors.newFixedThreadPool(50);
		for(int i=0;i<50;i++){
			pool.execute(new Runnable() {
				@Override
				public void run() {
					while(true){
						Map<String, String> data = new HashMap<String, String>();
						data.put("q", "SELECT MAX(value) FROM sensor where device_code='d_bt_0' and sensor_code='s_btg_8' and time>=1485541192230000000 and time<=1485627592230000000");
						HttpRequest.post("http://10.77.110.226:8086/query?db=ruc_test1").form(data).code();
					}
				}
			});
		}
		try {
			pool.awaitTermination(Long.MAX_VALUE,TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

