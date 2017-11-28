package cn.edu.ruc.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.github.kevinsawicki.http.HttpRequest;
import com.github.kevinsawicki.http.HttpRequest.ConnectionFactory;

public class HttpTest {
	   /**
  * 向指定 URL 发送POST方法的请求
  * 
  * @param url
  *            发送请求的 URL
  * @param param
  *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
  * @return 所代表远程资源的响应结果
  */
 public static String sendPost(String url, String param) {
     PrintWriter out = null;
     BufferedReader in = null;
     String result = "";
     try {
     	java.net.URL realUrl = new java.net.URL(url);
         // 打开和URL之间的连接
         URLConnection conn = realUrl.openConnection();
         // 设置通用的请求属性
         conn.setRequestProperty("accept", "*/*");
         conn.setRequestProperty("connection", "close");
         conn.setRequestProperty("user-agent",
                 "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
         // 发送POST请求必须设置如下两行
         conn.setDoOutput(true);
         conn.setDoInput(true);
         // 获取URLConnection对象对应的输出流
         out = new PrintWriter(conn.getOutputStream());
         // 发送请求参数
         out.print(param);
         // flush输出流的缓冲
         out.flush();
         // 定义BufferedReader输入流来读取URL的响应
         in = new BufferedReader(
                 new InputStreamReader(conn.getInputStream()));
         String line;
         while ((line = in.readLine()) != null) {
             result += line;
         }
     } catch (Exception e) {
         System.out.println("发送 POST 请求出现异常！"+e);
         e.printStackTrace();
     }
     //使用finally块来关闭输出流、输入流
     finally{
         try{
             if(out!=null){
                 out.close();
             }
             if(in!=null){
                 in.close();
             }
         }
         catch(IOException ex){
             ex.printStackTrace();
         }
     }
     return result;
 }
	private static CloseableHttpClient httpClient;
	public static String cleanEmoji(String s) {
		if (s == null) {
			return null;
		}
		String p = "[\ud83c\udc00-\ud83c\udfff]|[\ud83d\udc00-\ud83d\udfff]|[\u2600-\u27ff]";
		return s.replaceAll(p, "");
	}
	public static void main(String[] args) {
		String a="😱屏幕好像没有店家发的图看起来大，但总体还不错啊😱";
		String cleanEmoji = cleanEmoji(a);
		System.out.println(a);
		System.out.println(cleanEmoji);
//		System.out.println(testTry());;
//        System.out.printf("aaaaaaaaaaaa");
		String url="https://cc.0071515.com/";
//		String url="http://cc.0071515.com/sysinfo/online_kf";
		int sum=0;
//		for(int i=0;i<100;i++){
//			long startTime=System.currentTimeMillis();
//			int response = HttpRequest.get(url).code();
//			long endTime=System.currentTimeMillis();
//			sum+=(endTime-startTime);
//		}
//		System.out.println(sum/100+"|http-request");
		sum=0;
//		for(int i=0;i<100;i++){
//			long startTime1=System.currentTimeMillis();
//			sendPost(url, "");
//			long endTime1=System.currentTimeMillis();
//			sum+=(endTime1-startTime1);
//		}
//		System.out.println(sum/100+"|urlConnection");
//        HttpPoolManager.getHttpClient();
//        HttpPoolManager.getHttpClient();
//        HttpPoolManager.getHttpClient();
//        HttpPoolManager.getHttpClient();
//        HttpPoolManager.getHttpClient();
//        HttpPoolManager.getHttpClient();
//        HttpPoolManager.getHttpClient();
//        HttpPoolManager.getHttpClient();
        sum=0;
//        for(int i=0;i<100;i++){
//        	long startTime2=System.currentTimeMillis();
//        	CloseableHttpClient hc = HttpPoolManager.getHttpClient();
//        	HttpPost post = new HttpPost(url);
//        	try {
//        		hc.execute(post);
//        	} catch (Exception e) {
//        		e.printStackTrace();
//        	}
//        	long endTime2=System.currentTimeMillis();
//        	sum+=(endTime2-startTime2);
//        }
//        System.out.println(sum/100+"|httpClient");
	}
	public static String testTry(){
		try {
			System.out.println("11111111111");
//			int i=1/0;
			return "hhhh";
		} catch (Exception e) {
//			e.printStackTrace();
			System.out.println("222222222");
			return "333333";
		}finally{
			System.out.println("===============");
		}
//		return "";
	}
	private static CloseableHttpClient getHttpClient() {
		HttpClientBuilder hb = HttpClientBuilder.create();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(5000)
				.setConnectionRequestTimeout((int) TimeUnit.HOURS.toMillis(1))
				.setSocketTimeout((int) TimeUnit.HOURS.toMillis(1)).setExpectContinueEnabled(false).build();
		hb.setDefaultRequestConfig(config);
		CloseableHttpClient hc = hb.build();
		return hc;
	}
	public static void test(){
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		// Increase max total connection to 200  
		cm.setMaxTotal(200);  
		// Increase default max connection per route to 20  
		cm.setDefaultMaxPerRoute(20);  
		// Increase max connections for localhost:80 to 50  
		HttpHost localhost = new HttpHost("http://cc.0071515.com", 80);  
		cm.setMaxPerRoute(new HttpRoute(localhost), 2);  
		  
//		CloseableHttpClient httpClient = HttpClients.custom()  
//		        .setConnectionManager(cm)  
//		        .build();  
		httpClient = HttpClients.custom()  
				.setConnectionManager(cm)  
				.build();  
	}
}

