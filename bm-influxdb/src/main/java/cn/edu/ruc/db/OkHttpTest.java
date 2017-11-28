package cn.edu.ruc.db;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class OkHttpTest {
	private static final OkHttpClient client = new OkHttpClient().newBuilder()
		       .readTimeout(500, TimeUnit.MILLISECONDS)
		       .build();
	public static void main(String[] args) throws IOException {
		int count=1;
		ExecutorService pool = Executors.newFixedThreadPool(128);
		while (count<=10) {
			pool.execute(new Runnable() {
				@Override
				public void run() {
					for(int i=0;i<1000;i++){
//						RequestBody formBody =new FormBody.Builder()
//			            .add("q",selectSql)
//			            .build();
//						Request request = new Request.Builder()
//						.url(QUERY_URL)
//						.header("Connection", "close")
//						.post(formBody)
//						.build();
//						client.newCall(request).execute();
						Request request = new Request.Builder()
						.url("http://cc.0071515.com/")
						.header("Connection", "close")
						.build();
						long st = System.currentTimeMillis();
						try {
							Response response = client.newCall(request).execute();
						} catch (IOException e) {
							e.printStackTrace();
						}
						long et = System.currentTimeMillis();
						System.out.println(et-st);
//						client.dispatcher().executorService().shutdown();
						try {
							 client.connectionPool().evictAll();
						} catch (Exception e) {
//							e.printStackTrace();
						}
						long startTime = System.currentTimeMillis();
						System.currentTimeMillis();
					}
				}
			});
			count++;
		}
//	    if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
//	 
//	    Headers responseHeaders = response.headers();
//	    for (int i = 0; i < responseHeaders.size(); i++) {
//	      System.out.println(responseHeaders.name(i) + ": " + responseHeaders.value(i));
//	    }
//	 
//	    System.out.println(response.body().string());
	}
}

