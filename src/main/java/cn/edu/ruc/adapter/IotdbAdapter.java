package cn.edu.ruc.adapter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;

import cn.edu.ruc.base.Status;
import cn.edu.ruc.base.TsDataSource;
import cn.edu.ruc.base.TsPackage;
import cn.edu.ruc.base.TsParamConfig;
import cn.edu.ruc.base.TsQuery;
import cn.edu.ruc.base.TsWrite;
/**
 * iotdb 适配器
 * @author fasape
 *
 */
public class IotdbAdapter implements DBAdapter {
	private static String DRIVER_CLASS ="cn.edu.tsinghua.iotdb.jdbc.TsfileDriver";
	private static String URL ="jdbc:tsfile://%s:%s/";
	private static String USER ="";
	private static String PASSWD ="";
	private static final String ROOT_SERIES_NAME="root.perform";
	private Logger logger=LoggerFactory.getLogger(getClass());
	private TsParamConfig tspc=null;
	@Override
	public void initDataSource(TsDataSource ds,TsParamConfig tspc) {
		// TODO Auto-generated method stub
		this.tspc=tspc;
		//初始化连接
		try {
			Class.forName(DRIVER_CLASS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		URL=String.format(URL,ds.getIp(),ds.getPort());
		USER=ds.getUser();
		PASSWD=ds.getPasswd();
		//初始化数据库
		getDataSource();
		//初始化存储组
		if(tspc.getTestMode().equals("write")) {
			initTimeseriesAndStorage(tspc);
		}
	}
    /**
     * 初始化时间序列，并设置storage
     */
	private void initTimeseriesAndStorage(TsParamConfig tspc) {
		int deviceNum = tspc.getDeviceNum()*tspc.getWriteClients();
		int sensorNum = tspc.getSensorNum();
		deviceNum=150*500;
		Connection connection = null;
		Statement statement = null;
		try {
		    connection = getConnection();
		    statement = connection.createStatement();
		    try {
			    	String setStorageSql="SET STORAGE GROUP TO "+ROOT_SERIES_NAME;
			    	statement.execute(setStorageSql);
				for(int deviceIdx=0;deviceIdx<deviceNum;deviceIdx++) {
			    		try {
				    			String deviceCode="d_"+deviceIdx;
				    			for(int sensorIdx=0;sensorIdx<sensorNum;sensorIdx++) {
				    				String sensorCode="s_"+sensorIdx;
				    				String sql="CREATE TIMESERIES "+ROOT_SERIES_NAME+"."+deviceCode+"."+sensorCode+"  WITH DATATYPE=FLOAT, ENCODING=RLE";
				    				statement.addBatch(sql);
				    			}
				    			statement.executeBatch();
				    			statement.clearBatch();
				    			logger.info("{} create timeseries finished[{}/{}].",deviceCode,deviceIdx+1,deviceNum);
						} catch (Exception e) {
							e.printStackTrace();
						}
			    }
			} catch (Exception e) {
				e.printStackTrace();			}
//		    for(int deviceIdx=0;deviceIdx<deviceNum;deviceIdx++) {
//		    		try {
//			    			String deviceCode="d_"+deviceIdx;
//			    			for(int sensorIdx=0;sensorIdx<sensorNum;sensorIdx++) {
//			    				String sensorCode="s_"+sensorIdx;
//			    				String sql="CREATE TIMESERIES "+ROOT_SERIES_NAME+"."+deviceCode+"."+sensorCode+"  WITH DATATYPE=FLOAT, ENCODING=RLE";
//			    				statement.addBatch(sql);
//			    			}
//			    			statement.executeBatch();
//			    			statement.clearBatch();
//			    			logger.info("{} create timeseries finished[{}/{}].",deviceCode,deviceIdx+1,deviceNum);
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//		    }
		  } catch (Exception e) {
			  e.printStackTrace();
		  } finally {
			  closeStatement(statement);
			  closeConnection(connection);
		  }
	}
	private  Connection getConnection(){
		Connection connection=null;
		 try {
//			connection = DriverManager.getConnection(URL, USER, PASSWD);
			 //数据源管理
			 connection=getDataSource().getConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
		 return connection;
	}
	private void closeConnection(Connection conn){
		try {
			if(conn!=null){
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private void closeStatement(Statement statement){
		try {
			if(statement!=null){
				statement.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Override
	public Object preWrite(TsWrite tsWrite) {
		List<String> sqls=new ArrayList<>();
		LinkedList<TsPackage> pkgs = tsWrite.getPkgs();
		StringBuffer sqlBuffer=new StringBuffer();
		for(TsPackage pkg:pkgs) {
			StringBuffer valueBuffer=new StringBuffer();
			sqlBuffer.append("insert into ");
			sqlBuffer.append(ROOT_SERIES_NAME);
			sqlBuffer.append(".");
			String deviceCode = pkg.getDeviceCode();
			sqlBuffer.append(deviceCode);
			sqlBuffer.append("(");
			sqlBuffer.append("timestamp");
			Set<String> sensorCodes = pkg.getSensorCodes();
			valueBuffer.append("(");
			valueBuffer.append(pkg.getTimestamp());
			for(String sensorCode:sensorCodes) {
				sqlBuffer.append(",");
				sqlBuffer.append(sensorCode);
				valueBuffer.append(",");
				valueBuffer.append(pkg.getValue(sensorCode));
			}
			sqlBuffer.append(") values");
			valueBuffer.append(")");
			sqlBuffer.append(valueBuffer);
			sqls.add(sqlBuffer.toString());
			sqlBuffer.setLength(0);
		}
		return sqls;
	}

	@Override
	public Status execWrite(Object write) {
		@SuppressWarnings("unchecked")
		List<String> sqls=(List<String>)write;
		Connection connection = getConnection();
		Statement statement = null;
		Long costTime=0L;
		try {
			statement = connection.createStatement();
			for(String sql:sqls) {
				statement.addBatch(sql);
			}
			long startTime=System.nanoTime();
			statement.executeBatch();
			long endTime=System.nanoTime();
			costTime=endTime-startTime;
			statement.clearBatch();
		} catch (Exception e) {
			e.printStackTrace();
			return Status.FAILED(-1L);
		}finally {
			closeStatement(statement);
			closeConnection(connection);
		}
		return Status.OK(costTime);
	}

	@Override
	public Object preQuery(TsQuery tsQuery) {
		// TODO Auto-generated method stub
		
		StringBuffer sc=new StringBuffer();
		sc.append("select ");
		switch (tsQuery.getQueryType()) {
		case 1://简单查询
			sc.append(tsQuery.getSensorName());
			sc.append(" ");
			break;
		case 2://分析查询
			sc.append("");
			if(tsQuery.getAggreType()==1) {
				sc.append("max_value(");
			}
			if(tsQuery.getAggreType()==2) {
				sc.append("min_value(");
			}
			if(tsQuery.getAggreType()==3) {
				sc.append("mean(");
			}
			if(tsQuery.getAggreType()==4) {
				sc.append("count(");
			}
			sc.append(tsQuery.getSensorName());
			sc.append(") ");
			break;
		case 3://分析查询
			sc.append("");
			if(tsQuery.getAggreType()==1) {
				sc.append("max_value(");
			}
			if(tsQuery.getAggreType()==2) {
				sc.append("min_value(");
			}
			if(tsQuery.getAggreType()==3) {
				sc.append("mean(");
			}
			if(tsQuery.getAggreType()==4) {
				sc.append("count(");
			}
			sc.append(tsQuery.getSensorName());
			sc.append(") ");
			break;
		default:
			break;
		}
		sc.append("from ");
		if(tsQuery.getQueryType()==3){
			String template=ROOT_SERIES_NAME+".%s";
			List<String> devices = tsQuery.getDevices();
			for(int index=0;index<devices.size();index++){
				sc.append(String.format(template, devices.get(index)));
				if(index<(devices.size()-1)){
					sc.append(",");
				}else{
					sc.append(" ");
				}
			}
		}else{
			sc.append(ROOT_SERIES_NAME);
			sc.append(".");
			sc.append(tsQuery.getDeviceName());
			sc.append(" ");
		}
		if(tsQuery.getStartTimestamp()!=null) {
			sc.append("and ");
			sc.append("time >=");
			sc.append(tsQuery.getStartTimestamp());
			sc.append(" ");
		}
		if(tsQuery.getEndTimestamp()!=null) {
			sc.append("and ");
			sc.append("time <=");
			sc.append(tsQuery.getEndTimestamp());
			sc.append(" ");
		}
		if(tsQuery.getSensorLtValue()!=null) {
			sc.append("and ");
			sc.append(tsQuery.getSensorName());
			sc.append(">=");
			sc.append(tsQuery.getSensorLtValue());
			sc.append(" ");
		}
		if(tsQuery.getSensorGtValue()!=null) {
			sc.append("and ");
			sc.append(tsQuery.getSensorName());
			sc.append("<=");
			sc.append(tsQuery.getSensorGtValue());
			sc.append(" ");
		}
		if(tsQuery.getGroupByUnit()!=null&&tsQuery.getQueryType()==2) {
			sc.append("group by ");
			switch (tsQuery.getGroupByUnit()) {
			case 1:
				sc.append(" (1s,[");
				break;
			case 2:
				sc.append(" (1m,[");
				break;
			case 3:
				sc.append(" (1h,[");
				break;
			case 4:
				sc.append(" (1d,[");
				break;
			case 5:
				sc.append(" (1M,[");
				break;
			case 6:
				sc.append(" (1y,[");
				break;
			default:
				break;
			}
			sc.append(tsQuery.getStartTimestamp());
			sc.append(",");
			sc.append(tsQuery.getEndTimestamp());
			sc.append("])");
		}
		return sc.toString().replaceFirst("and", "where");
	}

	@Override
	public Status execQuery(Object query) {
		Connection conn=getConnection();
		Statement statement=null;
		long costTime=0;
		try {
			statement=conn.createStatement();
			long startTime=System.nanoTime();
//			System.out.println(query);
			ResultSet rs = statement.executeQuery(query.toString());
			rs.next();
			long endTime=System.nanoTime();
//			if(rs.next()){
//				System.out.println(rs.getObject(1));
//			}
			costTime=endTime-startTime;
//			System.out.println(query.toString()+"============"+costTime/1000+" us=============");
		} catch (SQLException e) {
			e.printStackTrace();
			return Status.FAILED(-1);
		}finally{
			closeStatement(statement);
			closeConnection(conn);
		}
		return Status.OK(costTime, 1);
	}
    public static void main(String[] args) {
		DBAdapter adapter=new IotdbAdapter();
		TsQuery query=new TsQuery();
		query.setQueryType(1);
		query.setDeviceName("d_1");
		query.setSensorName("s_49");
		query.setAggreType(2);
		query.setStartTimestamp(System.currentTimeMillis()-10000000L);
		query.setEndTimestamp(System.currentTimeMillis());
		query.setGroupByUnit(3);
		Object preQuery = adapter.preQuery(query);
		System.out.println(preQuery);
	}
    
    private  DruidDataSource dataSource;
    private  DruidDataSource getDataSource(){
	    	if(dataSource==null){
	    		synchronized (IotdbAdapter.class) {
	    			if(dataSource==null){
	    				dataSource = new DruidDataSource();  
	    				dataSource.setUsername(USER);  
	    				dataSource.setUrl(URL);  
	    				dataSource.setPassword(PASSWD);  
	    				dataSource.setDriverClassName(DRIVER_CLASS);  
	    				dataSource.setInitialSize(tspc.getWriteClients()+tspc.getReadClients()); 
	    				dataSource.setMaxActive(65535);  
	    				dataSource.setMaxWait(1000);  
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
	@Override
	public void closeAdapter() {
		// TODO Auto-generated method stub
		
	}
}
