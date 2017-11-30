package cn.edu.ruc.db;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class OkHttpTest {
	private static final OkHttpClient client = new OkHttpClient().newBuilder()
		       .readTimeout(500000, TimeUnit.MILLISECONDS)
		       .connectTimeout(500000, TimeUnit.MILLISECONDS)
		       .writeTimeout(500000, TimeUnit.MILLISECONDS)
		       .build();
	public static void main(String[] args) throws Exception {
		int count=1;
		ExecutorService pool = Executors.newFixedThreadPool(128);
		System.out.println("5555555555555555:"+client.connectionPool().connectionCount());
		while (count<=10) {
			pool.execute(new Runnable() {
				@Override
				public void run() {
					for(int i=0;i<100;i++){
//						RequestBody formBody =new FormBody.Builder()
//			            .add("q",selectSql)
//			            .build();
//						Request request = new Request.Builder()
//						.url(QUERY_URL)
//						.header("Connection", "close")
//						.post(formBody)
//						.build();
//						client.newCall(request).execute();
						MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
						String postBody = "Hello World";
						Request request = new Request.Builder()
						.url("http://cc.0071515.com/")
//						.header("Connection", "close")
						.post(RequestBody.create(MEDIA_TYPE_TEXT, postBody))
						.build();
						long st = System.currentTimeMillis();
						try {
							Response response = client.newCall(request).execute();
							System.out.println(response.code());
							response.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
						long et = System.currentTimeMillis();
						System.out.println(client.connectionPool().connectionCount());
//						System.out.println(et-st);
//						client.dispatcher().executorService().shutdown();
//						try {
//							 client.connectionPool().evictAll();
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//						long startTime = System.currentTimeMillis();
//						System.currentTimeMillis();
					}
				}
			});
			count++;
		}
		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.DAYS);
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

