package cn.edu.ruc.db;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.net.ssl.SSLContext;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;


/**
 * http管理池
 * @author sxg
 */
public class HttpPoolManager {
	//FIXME DBParam 管理参数
    static PoolingHttpClientConnectionManager cm = null;
    
    @PostConstruct
    public static void  init() {
        LayeredConnectionSocketFactory sslsf = null;
        try {
            sslsf = new SSLConnectionSocketFactory(SSLContext.getDefault());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create()
                .register("https", sslsf)
                .register("http", new PlainConnectionSocketFactory())
                .build();
        cm =new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        cm.setMaxTotal(60000);
        cm.setDefaultMaxPerRoute(60000);
    }
    static{
    	init();
    }
    public static CloseableHttpClient httpClient;
    public static CloseableHttpClient  getHttpClient() {       
//        CloseableHttpClient httpClient = HttpClients.custom()
//                .setConnectionManager(cm)
//                .build();          
//        return httpClient;
    	if(httpClient==null){
    		synchronized (HttpPoolManager.class) {
    			if(httpClient==null){
//    				ConnectionConfig config = ConnectionConfig.custom()
//    		                .setBufferSize(4128)
//    		                .build();
    				httpClient = HttpClients.custom()
    						.setConnectionManager(cm)
//    						.setDefaultConnectionConfig(config)
    						.build();       
    			}
			}
    	}
    	return httpClient;
    }
    private static int count;
    public static void main(String[] args) throws InterruptedException {
    	ExecutorService pool = Executors.newFixedThreadPool(100);
    	for(int i=0;i<1000;i++){
    		pool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						executeHttp();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
    		
    	}
        Thread.sleep(1000000L);
    }
    public static void executeHttp() throws InterruptedException{
    	HttpPoolManager connManager=new HttpPoolManager();
    	connManager.init();
        CloseableHttpClient httpClient=connManager.getHttpClient();
        HttpGet httpget = new HttpGet("http://cc.0071515.com/");
        String json=null;        
        CloseableHttpResponse response=null;
        try {
            response = httpClient.execute(httpget);
            InputStream in=response.getEntity().getContent();
            System.out.println(EntityUtils.toString(response.getEntity()));
            System.out.println(count++);
//            json=IOUtils.toString(in);
//            Thread.sleep(1000000L);
            in.close();
        } catch (UnsupportedOperationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {            
        	System.out.println("9999999999");
            if(response!=null){
            try {
                response.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            }            
        }
    }
}

