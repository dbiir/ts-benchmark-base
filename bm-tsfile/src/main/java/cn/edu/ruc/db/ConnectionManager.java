package cn.edu.ruc.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.druid.pool.DruidDataSource;

/**
 * 
 * @author sxg
 */
public class ConnectionManager {
	
	//初始连接数，最大连接数，最小连接数，改为统一配置的
	
    // 数据库驱动名称  
    public static String driver = "com.mysql.jdbc.Driver";  
    // 数据库连接地址  
    public static String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/pool_test?useUnicode=true&characterEncoding=UTF8&connectTimeout=1000&socketTimeout=3000";  
    // 数据库用户名  
    public static String user = "root";  
    // 数据库密码  
    public static String passwd = "1230";  
    // 连接池初始化大小  
    public static int initialSize = 10;  
    // 连接池最小空闲  
    public static int minPoolSize = 50;  
    // 连接池最大连接数量  
    public static int maxPoolSize = 15000;  
    // 最小逐出时间，100秒  
    public static int maxIdleTime = 100000;  
    // 连接失败重试次数  
    public static int retryAttempts = 10;  
    // 当连接池连接耗尽时获取连接数  
    public static int acquireIncrement = 5;  
    private static DruidDataSource dataSource;
    
    //http://blog.csdn.net/xzknet/article/details/49127701
    private static DruidDataSource getDataSource(){
    	if(dataSource==null){
    		synchronized (ConnectionManager.class) {
    			if(dataSource==null){
    				dataSource = new DruidDataSource();  
    				dataSource.setUsername(user);  
    				dataSource.setUrl(jdbcUrl);  
    				dataSource.setPassword(passwd);  
    				dataSource.setDriverClassName(driver);  
    				dataSource.setInitialSize(initialSize);  
    				dataSource.setMaxActive(maxPoolSize);  
    				dataSource.setMaxWait(maxIdleTime);  
    				dataSource.setTestWhileIdle(false);  
    				dataSource.setTestOnReturn(false);  
    				dataSource.setTestOnBorrow(false);  
    				dataSource.setDefaultAutoCommit(false);
    				dataSource.setDefaultReadOnly(false);
    			}
			}
    	}
    	return dataSource;
    }
    /**
     * druid管理连接
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException{
    	return getDataSource().getConnection();
    }
    public static void main(String[] args) throws SQLException, InterruptedException {
    	ExecutorService pool = Executors.newFixedThreadPool(100);
    	for(int i=0;i<100;i++){
    		pool.execute(new Runnable() {
				
				@Override
				public void run() {
					// TODO Auto-generated method stub
					try {
						Connection connection = getConnection();
						connection.createStatement();
						Thread.sleep(1000000L);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
    		
    	}
		Thread.sleep(100000L);
	}
}

