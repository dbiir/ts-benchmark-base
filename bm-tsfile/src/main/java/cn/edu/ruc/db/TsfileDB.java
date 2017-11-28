package cn.edu.ruc.db;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cn.edu.ruc.biz.Constants;
import cn.edu.ruc.biz.Core;
import cn.edu.ruc.biz.StressAppend;

/**
 * Hello world!
 *
 */
public class TsfileDB extends DBBase
{
	private static final String DB_URL_TSFILEDB_PROPERTY="tsfile.url";
	private static final String DB_USER_TSFILEDB_PROPERTY="tsfile.user";
	private static final String DB_PASSWD_TSFILEDB_PROPERTY="tsfile.passwd";
//	private static String DRIVER_CLASS ="cn.edu.thu.tsfiledb.jdbc.TsfileDriver";
	private static String DRIVER_CLASS ="cn.edu.tsinghua.iotdb.jdbc.TsfileDriver";
	private static String URL ="";
	private static String USER ="";
	private static String PASSWD ="";
	private static final String ROOT_SERIES_NAME="root.perform";
    public static void main( String[] args ) throws Exception
    {
        Core.main(args);
//        StressAppend.main(args);
//    	double a=2222221888888.66666685544545;
//    	if((Double)a instanceof Double){
//    		BigDecimal bd=new BigDecimal(a);
//    		System.out.println(bd.toString() );
//    	}
//    	System.out.println(a);
//    	TsfileDB db=new TsfileDB();
//    	db.init();
//    	Status status = db.selectCountByDeviceAndSensor("d_iq_0","s_tfi_335",new Date(0L),new Date());
//    	System.out.println(status.getCostTime());
    }
    @Override
    public void init() {
    	super.init();
    	try {
			Properties properties = getProperties();
			URL=properties.getProperty(DB_URL_TSFILEDB_PROPERTY, "jdbc:tsfile://192.168.130.17:6667/");
			USER=properties.getProperty(DB_USER_TSFILEDB_PROPERTY, "root");
			PASSWD=properties.getProperty(DB_PASSWD_TSFILEDB_PROPERTY, "root");
			Class.forName(DRIVER_CLASS);
			ConnectionManager.user=USER;
			ConnectionManager.passwd=PASSWD;
			ConnectionManager.jdbcUrl=URL;
			ConnectionManager.driver=DRIVER_CLASS;
			initTimeseriesAndStorage();//初始化时间序列，并设置storage
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
    }
    /**
     * 初始化时间序列，并设置storage
     */
	private void initTimeseriesAndStorage() {
		List<String> devices = Constants.getAllDevices();
		List<String> sensors=Constants.getAllSensors();
		Connection connection = null;
		Statement statement = null;
		try {
		    connection = getConnection();
		    statement = connection.createStatement();
			for(String device:devices){
				for(String sensor:sensors){
					String sql="CREATE TIMESERIES "+ROOT_SERIES_NAME+"."+device+"."+sensor+"  WITH DATATYPE=FLOAT, ENCODING=RLE";
					statement.addBatch(sql);
				}
			}
			String setStorageSql="SET STORAGE GROUP TO "+ROOT_SERIES_NAME;
		    statement.executeBatch();
		    statement.clearBatch();
		    statement.execute(setStorageSql);
		  } catch (Exception e) {
			  e.printStackTrace();
		  } finally {
			    if(statement != null){
			      try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		    }
		    closeConnection(connection);
		  }
	}
	private  Connection getConnection(){
		Connection connection=null;
		 try {
//			connection = DriverManager.getConnection(URL, USER, PASSWD);
			 //数据源管理
			 connection=ConnectionManager.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		 return connection;
	}
	private void closeConnection(Connection conn){
		try {
			if(conn!=null){
				conn.close();
			}
		} catch (SQLException e) {
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
	public Status insertMulti(List<TsPoint> points) {
		//timestamp->device->sensor
		long costTime=0;
//		int count=0;
		Connection connection = null;
		Statement statement = null;
		try {
			connection=getConnection();
			statement=connection.createStatement();
			if(points!=null){
				String sqlHeader="insert into "+ROOT_SERIES_NAME+".";
				StringBuilder sqlBuiulder=new StringBuilder();
				StringBuilder keyBuiulder=new StringBuilder();
				StringBuilder valueBuiulder=new StringBuilder();
				int size=points.size();
				for(int i=0;i<size;i++){
					TsPoint point=points.get(i);
					if(point==null){
						continue;
					}
					//是否是一个时间点一个设备的第一个点
					boolean isFirstPoint=false;
					//判断是否同一个时间点同一个设备的第一个点
					if(i==0){
						isFirstPoint=true;
					}else{
						TsPoint nextTsPoint=points.get(i-1);
						if(!point.isSameDeviceAndTime(nextTsPoint)){
							isFirstPoint=true;
						}
					}
					
					//是否是一个时间点一个设备的最后一个点
					boolean isLastPoint=false;
					//判断是否同一个时间点同一个设备的最后一个点
					if(i==size-1){
						isLastPoint=true;
					}else{
						TsPoint nextTsPoint=points.get(i+1);
						if(!point.isSameDeviceAndTime(nextTsPoint)){
							isLastPoint=true;
						}
					}
					
					if(isFirstPoint){
						sqlBuiulder.append(sqlHeader);
						sqlBuiulder.append(point.getDeviceCode());
						keyBuiulder.append("(");
						keyBuiulder.append("timestamp");
						valueBuiulder.append("(");
						valueBuiulder.append(point.getTimestamp());
					}
					keyBuiulder.append(",");
					keyBuiulder.append(point.getSensorCode());
					valueBuiulder.append(",");
					Object value = point.getValue();
					String valueStr=value.toString();
					if(value instanceof Double){
						BigDecimal bd=new BigDecimal((double)value);
						valueStr=bd.toString();
					}
					valueBuiulder.append(valueStr);
					
					if(isLastPoint){
						keyBuiulder.append(")");
						keyBuiulder.append(" values");
						sqlBuiulder.append(keyBuiulder);
						valueBuiulder.append(")");
						sqlBuiulder.append(valueBuiulder);
						keyBuiulder.setLength(0);
						valueBuiulder.setLength(0);
//						System.out.println(sqlBuiulder.toString());
						statement.addBatch(sqlBuiulder.toString());
//						System.out.println(sqlBuiulder.toString());
						sqlBuiulder.setLength(0);
					}
//				long currentTimestamp = point.getTimestamp();
//				
//				if(!timestampList.contains(currentTimestamp)){
//					timestampList.add(currentTimestamp);
//				}
//				Map<String, Map<String, Object>> deviceMap = dataMap.get(currentTimestamp);
//				if(deviceMap==null){
//					deviceMap=new HashMap<String, Map<String,Object>>();
//					dataMap.put(currentTimestamp, deviceMap);
//				}
//				String deviceCode = point.getDeviceCode();
//				Map<String, Object> sensorMap = deviceMap.get(deviceCode);
//				if(sensorMap==null){
//					sensorMap=new HashMap<String, Object>();
//					deviceMap.put(deviceCode, sensorMap);
//				}
//				String sensorCode = point.getSensorCode();
//				Object value = point.getValue();
//				sensorMap.put(sensorCode, value);
//					count++;
//					if(count%1000==0){
//						System.out.println("count=="+count);
//					}
				}
			}
			long startTime=System.nanoTime();
			statement.executeBatch();//批量写入
			statement.clearBatch();
			long endTime=System.nanoTime();
			costTime=endTime-startTime;
//			System.out.println("批量插入消耗时间["+(endTime-startTime)+"ms]");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			closeStatement(statement);
			closeConnection(connection);
		}
//		Status status=insertByDataMap(dataMap,timestampList);
		return Status.OK(costTime);
	}

	private Status insertByDataMap(
			Map<Long, Map<String, Map<String, Object>>> dataMap,List<Long> seqList) {
		if(dataMap!=null&&seqList!=null){
			for(Long timestamp:seqList){
				Map<String, Map<String, Object>> deviceMap = dataMap.get(timestamp);
				Set<String> deviceCodeSet = deviceMap.keySet();
				for(String deviceCode:deviceCodeSet){
					Map<String, Object> sensorMap = deviceMap.get(deviceCode);
					Set<String> keySet = sensorMap.keySet();
					StringBuilder sql=new StringBuilder("insert into "+ROOT_SERIES_NAME+"."+deviceCode+"");
					StringBuilder keyBuiulder=new StringBuilder();
					StringBuilder valueBuiulder=new StringBuilder();
					keyBuiulder.append("(");
					keyBuiulder.append("timestamp");
					valueBuiulder.append("(");
					valueBuiulder.append(timestamp);
					for(String sensorCode:keySet){
						keyBuiulder.append(",");
						keyBuiulder.append(sensorCode);
						valueBuiulder.append(",");
						valueBuiulder.append(sensorMap.get(sensorCode));
					}
					keyBuiulder.append(")");
					valueBuiulder.append(")");
					sql.append(keyBuiulder);
					sql.append(" ");
					sql.append("values");
					sql.append(valueBuiulder);
					System.out.println("+++++++++++++++++++++++++++++++");
					System.out.println(sql);
					System.out.println("+++++++++++++++++++++++++++++++");
				}
			}
		}
		return null;
	}
	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Date startTime,
			Date endTime) {
		String sql="select "+point.getSensorCode()+" from "+ROOT_SERIES_NAME+"."+point.getDeviceCode()+" where time>="+startTime.getTime()+" and time<="+endTime.getTime();
//		String sql="select "+point.getSensorCode()+" from "+ROOT_SERIES_NAME+"."+point.getDeviceCode()+" where time>="+startTime.getTime()+" and time<="+endTime.getTime();
		return execBySql(sql);
	}

	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Double max,
			Double min, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectByDevice(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectDayMaxByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectDayMinByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectDayAvgByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectHourMaxByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectHourMinByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectHourAvgByDevice(TsPoint point, Date startTime,
			Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectMinuteMaxByDeviceAndSensor(TsPoint point,
			Date startTime, Date endTime) {
		return null;
	}

	@Override
	public Status selectMinuteMinByDeviceAndSensor(TsPoint point,
			Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectMinuteAvgByDeviceAndSensor(TsPoint point,
			Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status updatePoints(List<TsPoint> points) {
		//timestamp->device->sensor
		long costTime=0;
//		int count=0;
		Connection connection = null;
		Statement statement = null;
		try {
			connection=getConnection();
			statement=connection.createStatement();
			if(points!=null){
				StringBuilder sc=new StringBuilder();
				for(TsPoint point:points){
					sc.append("update ");
					sc.append(ROOT_SERIES_NAME);
					sc.append(".");
					sc.append(point.getDeviceCode());
					sc.append(".");
					sc.append(point.getSensorCode());
					sc.append(" set value=");
					sc.append(point.getValue());
					sc.append(" where");
					sc.append(" time=");
					sc.append(point.getTimestamp());
					statement.addBatch(sc.toString());
					sc.setLength(0);
				}
			}
			long startTime=System.nanoTime();
			statement.executeBatch();//批量更新
			statement.clearBatch();
			long endTime=System.nanoTime();
			costTime=endTime-startTime;
		}catch(Exception e){
			return Status.FAILED(-1);
		}finally{
			closeStatement(statement);
			closeConnection(connection);
		}
		return Status.OK(costTime);
	}

	@Override
	public Status deletePoints(Date date) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Status selectMaxByDeviceAndSensor(String deviceCode,
			String sensorCode, Date startTime, Date endTime) {
		String sql="select max_value("+sensorCode+") from "+ROOT_SERIES_NAME+"."+deviceCode+" where time>="+startTime.getTime()+" and time<="+endTime.getTime();
//		System.out.println(sql);
		return execBySql(sql);
	}
	@Override
	public Status selectMinByDeviceAndSensor(String deviceCode,
			String sensorCode, Date startTime, Date endTime) {
		String sql="select min_value("+sensorCode+") from "+ROOT_SERIES_NAME+"."+deviceCode+" where time>="+startTime.getTime()+" and time<="+endTime.getTime();
		return execBySql(sql);
	}
	@Override
	public Status selectAvgByDeviceAndSensor(String deviceCode,
			String sensorCode, Date startTime, Date endTime) {
		//FIXME avg目前不支持
		String sql="select avg_value("+sensorCode+") from "+ROOT_SERIES_NAME+"."+deviceCode+" where time>="+startTime.getTime()+" and time<="+endTime.getTime();
		return execBySql(sql);
	}
	@Override
	public Status selectCountByDeviceAndSensor(String deviceCode,
			String sensorCode, Date startTime, Date endTime) {
		String sql="select count("+sensorCode+") from "+ROOT_SERIES_NAME+"."+deviceCode +" where time>="+startTime.getTime()+" and time<="+endTime.getTime();
		return execBySql(sql);
	}
	public Status execBySql(String sql){
		Connection conn=getConnection();
		Statement statement=null;
		long costTime=0;
		try {
			statement=conn.createStatement();
			long startTime=System.nanoTime();
			ResultSet rs = statement.executeQuery(sql);
			long endTime=System.nanoTime();
//			while(rs.next()){
//				System.out.println(rs.getObject(1));
//			}
			costTime=endTime-startTime;
		} catch (SQLException e) {
			e.printStackTrace();
			return Status.FAILED(-1);
		}finally{
			closeStatement(statement);
			closeConnection(conn);
		}
		return Status.OK(costTime, 1);
	}
	@Override
	public String getDBUrl() {
		return URL;
	}
}
