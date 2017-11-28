package cn.edu.ruc.db;

import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

public class SessionManager {
	private static Cluster cluster = null;
	private static Session session=null;
	private static Session rootSession=null;
	public static String keyspaceName="ruc_perform";
	public static String url="127.0.0.1";
	public static int core=10;
	public static int max=10000;
	private static Cluster getCluster(){
		if(cluster==null){
			synchronized (SessionManager.class) {
				if(cluster==null){
					PoolingOptions poolingOptions = new PoolingOptions();
					poolingOptions
					.setMaxRequestsPerConnection(HostDistance.REMOTE, max)
					.setMaxRequestsPerConnection(HostDistance.LOCAL,max)
					.setMaxQueueSize(max*10)
					.setCoreConnectionsPerHost(HostDistance.LOCAL,  1)
					.setMaxConnectionsPerHost( HostDistance.LOCAL, 2)
					.setCoreConnectionsPerHost(HostDistance.REMOTE, 1)
					.setMaxConnectionsPerHost( HostDistance.REMOTE, 2);
					SocketOptions socketOptions = new SocketOptions();
					socketOptions.setConnectTimeoutMillis(60000);
					socketOptions.setReadTimeoutMillis(60000);
					cluster = Cluster.builder().addContactPoint(url).withPoolingOptions(poolingOptions).withSocketOptions(socketOptions).build();
					Metadata metadata = cluster.getMetadata();
					Set<Host> allHosts = metadata.getAllHosts();
					for(Host host:allHosts){
						System.out.println("host:"+host.getAddress());
					}
				}
			}
		}
		return cluster;
	}
	public static Session getSession(){
		if(session==null){
			synchronized (SessionManager.class) {
				if(session==null){
					System.out.println(session);
					session=getCluster().connect(keyspaceName);
				}
			}
		}
		return session;
	}
	public static Session getRootSession(){
		if(rootSession==null){
			synchronized (SessionManager.class) {
				if(rootSession==null){
					rootSession=getCluster().connect();
				}
			}
		}
		return rootSession;
	}
	public static void closeSession(Session session){
		session.close();
	}
	public static void main(String[] args) throws InterruptedException {
	   	ExecutorService pool = Executors.newFixedThreadPool(1000);
    	for(int i=0;i<1000;i++){
    		long a=i;
    		pool.execute(new Runnable() {
				@Override
				public void run() {
					try {
//						Thread.sleep(a*100);
//						System.out.println(a);
//						Session session = getSession();
//						String sql="SELECT MAX(value) FROM point ALLOW FILTERING";
//						ResultSet rs = session.execute(sql);
//						Iterator<Row> it = rs.iterator();
//						while(it.hasNext()){
//							System.out.println(it.next().getObject(1));
//						}
						DBBase base=new CassandraDB();
						base.selectMaxByDeviceAndSensor("a","b", new Date(),new Date());
//						session.close();
//						Thread.sleep(100000L);
//						session.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
    		
    	}
        Thread.sleep(1000000L);
	}
	public static void closeCluster(){
		if(cluster!=null){
			cluster.close();
		}
	}
}

