package cn.edu.ruc.biz.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import cn.edu.ruc.biz.Constants;
import cn.edu.ruc.biz.model.LoadRecord;
import cn.edu.ruc.biz.model.ReadRecord;
import cn.edu.ruc.biz.model.RequestThroughputRecord;
import cn.edu.ruc.biz.model.RequestTimeout;
import cn.edu.ruc.biz.model.ThroughputRecord;
import cn.edu.ruc.biz.model.TimeoutRecord;
import cn.edu.ruc.biz.model.WriteRecord;
import cn.edu.ruc.enums.LoadTypeEnum;

/**
 * 业务操作工具类 
 */
public class BizDBUtils {
	public static Connection getConnection(){
		Connection conn=null;
	    try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:perform.db");
		} catch (Exception e) {
			e.printStackTrace();
		}
	    return conn;
	}
	public static void closeConnection(Connection conn){
		if(conn!=null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 通过sql插入数据
	 * @param sql
	 */
	public static void insertBySqlAndParam(String sql,Object[] params){
		Connection conn=null;
		conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				return 1;
			}
		};
		try {
			runner.insert(conn, sql,rsh,params );
		} catch (Exception e) {
			e.printStackTrace();
		}
		closeConnection(conn);
	}
	/**
	 * 通过sql插入数据
	 * @param sql
	 */
	public static void insertBySqlAndParam(Connection conn,String sql,Object[] params){
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				return 1;
			}
		};
		try {
			runner.insert(conn, sql,rsh,params );
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 通过表名称，列名称插入数据
	 * @param columns
	 * @param values
	 * @param tableName
	 * @return
	 */
	public static long insertBySqlAndParamAndTable(String[] columns,Object[] values,String tableName){
		Connection conn=null;
		conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				while (rs.next()) {
					return rs.getInt(1);
				}
				return 1;
			}
		};
		long key=-1;
		try {
			StringBuilder sc=new StringBuilder();
			sc.append("insert into ");
			sc.append(tableName);
			sc.append("(");
			int columnSize=columns.length;
			for(int i=0;i<columnSize;i++){
				sc.append(columns[i]);
				if(i<columnSize-1){
					sc.append(",");
				}
			}
			sc.append(") values(");
			for(int i=0;i<columnSize;i++){
				sc.append("?");
				if(i<columnSize-1){
					sc.append(",");
				}
			}
			sc.append(")");
//			System.out.println("sql:"+sc.toString());
			runner.insert(conn, sc.toString(),rsh,values);
			String keySql="select last_insert_rowid() from "+tableName;
			key = (long) runner.query(conn, keySql, rsh);
		} catch (Exception e) {
			e.printStackTrace();
		}
		closeConnection(conn);
		return key;
	}
	/**
	 * 通过表名称，列名称插入数据
	 * @param columns
	 * @param values
	 * @param tableName
	 * @return
	 */
	public static long insertBySqlAndParamAndTable(Connection conn,String[] columns,Object[] values,String tableName){
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				while (rs.next()) {
					return rs.getInt(1);
				}
				return 1;
			}
		};
		long key=-1;
		try {
			StringBuilder sc=new StringBuilder();
			sc.append("insert into ");
			sc.append(tableName);
			sc.append("(");
			int columnSize=columns.length;
			for(int i=0;i<columnSize;i++){
				sc.append(columns[i]);
				if(i<columnSize-1){
					sc.append(",");
				}
			}
			sc.append(") values(");
			for(int i=0;i<columnSize;i++){
				sc.append("?");
				if(i<columnSize-1){
					sc.append(",");
				}
			}
			sc.append(")");
//			System.out.println("sql:"+sc.toString());
			runner.insert(conn, sc.toString(),rsh,values);
			String keySql="select last_insert_rowid() from "+tableName;
			key = (long) runner.query(conn, keySql, rsh);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return key;
	}
	/**
	 * 通过sql创建表，表存在则不创建，不存在则创建
	 * @param sql
	 * @param tableName
	 */
	public static void createTableBySql(String sql,String tableName){
	    Connection c = null;
	    Statement stmt = null;
		try {
		    Class.forName("org.sqlite.JDBC");
		    c = DriverManager.getConnection("jdbc:sqlite:perform.db");
		    stmt = c.createStatement();
			String countSql="SELECT COUNT(*) num FROM sqlite_master where type='table' and LOWER(name)='"+tableName+"'";
			ResultSet set = stmt.executeQuery(countSql);
			while (set.next()) {
				int num = set.getInt("num");
				if(num==0){
					stmt.executeUpdate(sql);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		    try {
				c.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 创建数据表
	 */
	public static void createTables(){
	    String loadBatchSql = "CREATE TABLE ts_load_batch ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
	    		+"target_db text NOT NULL, "
//	    		+"starup_type text NOT NULL, "
	    		+"data_status INTEGER NOT NULL, "
	    		+"db_url text NOT NULL, "
	    		+"create_time INTEGER NOT NULL, "
	    		+"device_num INTEGER NOT NULL, "
	    		+"sensor_num INTEGER NOT NULL,"
	    		+"point_step INTEGER NOT NULL, "
	    		+"cache_point_num INTEGER NOT NULL,"
	    		+"point_lose_ratio REAL NOT NULL, "
	    		+"line_ratio REAL NOT NULL, "
	    		+"sin_ratio REAL NOT NULL, "
	    		+"square_ratio REAL NOT NULL, "
	    		+"random_ratio REAL NOT NULL, "
	    		+"constant_ratio REAL NOT NULL,"
	    		+"history_start_time INTEGER NOT NULL,"
	    		+"history_end_time INTEGER NOT NULL "
//	    		+"insert_perform_device_prefix text "
	    		+") ";
	    createTableBySql(loadBatchSql,"ts_load_batch");
	    
	    String performBatchSql="create table ts_perform_batch(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
	    		+"load_batch_id  INTEGER NOT NULL,"
	    		+"write_ratio NOT NULL,"
	    		+"simple_query_ratio NOT NULL,"
	    		+"max_query_ratio NOT NULL,"
	    		+"min_query_ratio NOT NULL,"
	    		+"avg_query_ratio	NOT NULL,"
	    		+"count_query_ratio NOT NULL,"
	    		+"sum_query_ratio NOT NULL,"
	    		+"random_insert_ratio NOT NULL,"
	    		+"update_ratio NOT NULL,"
	    		+"insert_perform_device_prefix text)";
	    createTableBySql(performBatchSql,"ts_perform_batch");
	    
	    String deviceInfoTable="CREATE TABLE ts_device_info ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
	    			+"create_time INTEGER NOT NULL,"
	    			+"name text NOT NULL,"
	    			+"load_batch_id INTEGER)";
	    createTableBySql(deviceInfoTable,"ts_device_info");
	    
	    String sensorInfoSql="CREATE TABLE ts_sensor_info ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
	    					+"create_time INTEGER NOT NULL, "
	    					+"name text text NOT NULL, "
	    					+"device_id INTEGER NOT NULL, "
	    					+"function_id text NOT NULL, "
	    					+"function_type text NOT NULL, "
	    					+"shift_time INTEGER NOT NULL)";
	    createTableBySql(sensorInfoSql,"ts_sensor_info");
	    
	    String loadSql="CREATE TABLE ts_load_record ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
	    				+"load_time INTEGER, "
	    				+"load_batch_id INTEGER, "
	    				+"load_points INTEGER, "
	    				+"load_size INTEGER, "
	    				+"load_cost_time INTEGER, "
	    				+"sps REAL,"
	    				+"pps INTEGER)";
	    createTableBySql(loadSql,"ts_load_record");
	    
	    String writeRecordSql="CREATE TABLE ts_write_record ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
	    					 +"write_time INTEGER, "
	    					 +"perform_batch_id INTEGER, "
	    					 +"target_dn_ps INTEGER,"
	    					 +"target_point_ps INTEGER, "
	    					 +"real_point_ps INTEGER)"; 
	    createTableBySql(writeRecordSql,"ts_write_record");
	    
	    String readRecordSql="CREATE TABLE ts_read_record ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
	    				+"read_time INTEGER, "
	    				+"perform_batch_id INTEGER, "
	    				+"target_tps INTEGER, "
	    				+"real_tps INTEGER, "
	    				+"avg_timeout INTEGER, "
	    				+"max_timeout INTEGER, "
						+"min_timeout INTEGER, "
						+"th95_timeout INTEGER, "
						+"read_type INTEGER, "
						+"read_type1_times INTEGER, "
						+"read_type2_times INTEGER, "
						+"read_type3_times INTEGER, "
						+"read_type4_times INTEGER, "
						+"read_type5_times INTEGER, "
						+"read_type6_times INTEGER, "
						+"read_type7_times INTEGER, "
						+"read_type8_times INTEGER, "
						+"read_type9_times INTEGER, "
						+"read_type10_times INTEGER, "
						+"read_type11_times INTEGER, "
						+"simple_max_read_times INTEGER, "
						+"simple_min_read_times INTEGER, "
						+"simple_avg_read_times INTEGER, "
						+"simple_count_read_times INTEGER, "
						+"read_type12_times INTEGER)";
	    createTableBySql(readRecordSql,"ts_read_record");
	    String updateRecordSql="CREATE TABLE ts_update_record ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
	    						+"read_time INTEGER, "
	    						+"perform_batch_id INTEGER, "
	    						+"target_pps INTEGER, "
	    						+"real_pps INTEGER)";
	    createTableBySql(updateRecordSql,"ts_read_record");
	    String randomInsertSql="CREATE TABLE ts_random_insert_record ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
	    						+"read_time INTEGER, "
	    						+"perform_batch_id INTEGER, "
	    						+"target_pps INTEGER, "
								+"real_pps INTEGER )";
	    createTableBySql(randomInsertSql,"ts_random_insert_record");
	    String deleteSql="CREATE TABLE ts_delete_record ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
	    			+"read_time INTEGER, "
	    			+"perform_batch_id INTEGER, "
	    			+"pps INTEGER, "
	    			+"sum_points INTEGER)";
	    createTableBySql(deleteSql,"ts_delete_record");
	    String writeLoadSql="CREATE TABLE ts_write_load_record ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
	    					+"write_time INTEGER, "
	    					+"perform_batch_id INTEGER, "
	    					+"target_dn_ps INTEGER, "
	    					+"target_point_ps INTEGER, "
	    					+"real_point_ps INTEGER )";
	    createTableBySql(writeLoadSql,"ts_write_load_record");
	    
	    String readLoadSql="CREATE TABLE ts_read_load_record ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
				+"read_time INTEGER, "
				+"perform_batch_id INTEGER, "
				+"target_tps INTEGER, "
				+"real_tps INTEGER, "
				+"avg_timeout INTEGER, "
				+"max_timeout INTEGER, "
				+"min_timeout INTEGER, "
				+"th95_timeout INTEGER, "
				+"read_type INTEGER, "
				+"read_type1_times INTEGER, "
				+"read_type2_times INTEGER, "
				+"read_type3_times INTEGER, "
				+"read_type4_times INTEGER, "
				+"read_type5_times INTEGER, "
				+"read_type6_times INTEGER, "
				+"read_type7_times INTEGER, "
				+"read_type8_times INTEGER, "
				+"read_type9_times INTEGER, "
				+"read_type10_times INTEGER, "
				+"read_type11_times INTEGER, "
				+"simple_max_read_times INTEGER, "
				+"simple_min_read_times INTEGER, "
				+"simple_avg_read_times INTEGER, "
				+"simple_count_read_times INTEGER, "
				+"read_type12_times INTEGER)";
	    createTableBySql(readLoadSql,"ts_read_load_record");
	    
	    //创建数据表ts_timeout_perform
	    String timeoutPerformSql = "create table ts_timeout_perform ("
	    				  +"id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
						  +"perform_batch_id INTEGER," 
						  +"time INTEGER,"
						  +"load_type INTEGER,"
						  +"target_times INTEGER,"
						  +"threads INTEGER,"
						  +"write_times INTEGER,"
						  +"random_insert_times INTEGER,"
						  +"simple_read_times INTEGER,"
						  +"aggre_read_times INTEGER,"
						  +"update_times INTEGER,"
						  +"timeout_max INTEGER,"
						  +"timeout_min INTEGER,"
						  +"timeout_avg INTEGER,"
						  +"timeout_th50 INTEGER,"
						  +"timeout_th95 INTEGER,"
						  +"timeout_sum INTEGER,"
						  +"success_times INTEGER,"
						  +"failed_times INTEGER)";
	    createTableBySql(timeoutPerformSql,"ts_timeout_perform");
	    
	    //请求吞吐量表
	    String requestsTpSql= "create table ts_requests_tp_perform ("
				  +"id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
				  +"perform_batch_id INTEGER," 
				  +"time INTEGER,"
				  +"load_type INTEGER,"
				  +"avg_cost_time REAL,"
				  +"request_times INTEGER,"
				  +"tps REAL,"
				  +"success_times INTEGER,"
				  +"failed_times INTEGER)";
	    createTableBySql(requestsTpSql,"ts_requests_tp_perform");
	    
	    //创建混合吞吐量测试表
	    String throughputSql="create table ts_throughput"
	    		+ "(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
	    		+ "perform_batch_id INTEGER NOT NULL,"
	    		+ "db_type text NOT NULL,"
	    		+ "sum_success_count INTEGER NOT NULL,"
	    		+ "cost_time INTEGER NOT NULL,"
	    		+ "ops INTEGER NOT NULL)";
	    createTableBySql(throughputSql,"ts_throughput");
	    String throughputDetailSql="create table ts_throughput_detail"
	    		+ "(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"
	    		+ "tt_id INTEGER NOT NULL,"
	    		+ "operate_type INTEGER NOT NULL,"
	    		+ "operate_times INTEGER NOT NULL,"
	    		+ "failed_times INTEGER NOT NULL,"
	    		+ "avg_latency INTEGER NOT NULL,"
	    		+ "min_latency INTEGER NOT NULL,"
	    		+ "max_latency INTEGER NOT NULL)";
	    createTableBySql(throughputDetailSql,"ts_throughput_detail");
	}
	
	public static void insertReadRecord(ReadRecord record){
		String[] columns={"read_time","perform_batch_id","target_tps","real_tps","avg_timeout",
						  "max_timeout","min_timeout","th95_timeout","read_type","read_type1_times",
						  "read_type2_times","read_type3_times","read_type4_times","read_type5_times","read_type6_times",
						  "read_type7_times","read_type8_times","read_type9_times","read_type10_times","read_type11_times",
						  "read_type12_times","simple_max_read_times","simple_min_read_times","simple_avg_read_times","simple_count_read_times"};
		Object[] values={record.getReadTime(),record.getBatchId(),record.getTargetTps(),record.getRealTps(),record.getAvgTimeOut(),
						record.getMaxTimeOut(),record.getMinTimeOut(),record.getTh95TimeOut(),record.getReadType(),record.getRead1Times(),
						record.getRead2Times(),record.getRead3Times(),record.getRead4Times(),record.getRead5Times(),record.getRead6Times(),
						record.getRead7Times(),record.getRead8Times(),record.getRead9Times(),record.getRead10Times(),record.getRead11Times(),
						record.getRead12Times(),record.getSimpleReadMaxTimes(),record.getSimpleReadMinTimes(),record.getSimpleReadAvgTimes(),record.getSimpleReadCountTimes()};
		BizDBUtils.insertBySqlAndParamAndTable(columns,values,"ts_read_record");
	}
	public static void insertTimeoutRecord(TimeoutRecord record){
		String[] columns={"perform_batch_id","time","load_type","target_times","threads","write_times",
						  "random_insert_times","simple_read_times","aggre_read_times","update_times","timeout_max","timeout_min",
						  "timeout_avg","timeout_th50","timeout_th95","timeout_sum","success_times","failed_times"};
		Object[] values={record.getBatchId(),record.getTime(),record.getLoadType(),record.getTargetTimes(),record.getThreads(),record.getWriteTimes(),
						record.getRandomInsertTimes(),record.getSimpleReadTimes(),record.getAggreReadTimes(),record.getUpdateTimes(),record.getTimeoutMax(),record.getTimeoutMin(),
						record.getTimeoutAvg(),record.getTimeoutTh50(),record.getTimeoutTh95(),record.getTimeoutSum(),record.getSuccessTimes(),record.getFailedTimes()};
		BizDBUtils.insertBySqlAndParamAndTable(columns,values,"ts_timeout_perform");
	}
	
	public static int selectCountBySql(String countSql) {
		Connection conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Integer> rsh=new ResultSetHandler<Integer>() {
			@Override
			public Integer handle(ResultSet rs) throws SQLException {
				while (rs.next()) {
					return rs.getInt(1);
				}
				return 1;
			}
		};
		int count=0;
		try {
			count=runner.query(conn,countSql, rsh);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}
	public static double selectSingleBySql(String avgSql) {
		Connection conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Double> rsh=new ResultSetHandler<Double>() {
			@Override
			public Double handle(ResultSet rs) throws SQLException {
				while (rs.next()) {
					return rs.getDouble(1);
				}
				return 0.0;
			}
		};
		Double avg=0.0;
		try {
			avg=runner.query(conn,avgSql, rsh);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(avg==null) avg=0.0;
		return avg;
	}
	public static void insertWriteRecord(WriteRecord record) {
		String[] columns={"write_time","perform_batch_id","target_dn_ps","target_point_ps","real_point_ps"};
		Object[] values={record.getWriteTime(),record.getBatchId(),record.getTargetDnPs(),record.getTargetPointPs(),record.getRealPointPs()};
		BizDBUtils.insertBySqlAndParamAndTable(columns,values,"ts_write_record");
	}
	public static void insertLoadRecord(LoadRecord record) {
		String[] columns={"load_time","load_batch_id","load_points","load_size","load_cost_time","pps","sps"};
		Object[] values={record.getLoadTime(),record.getBatchId(),record.getLoadPoints(),record.getLoadSize(),record.getLoadCostTime(),record.getPps(),record.getSps()};
		BizDBUtils.insertBySqlAndParamAndTable(columns,values,"ts_load_record");
	}
	public static void insertRequestTPRecord(RequestThroughputRecord record) {
		//				  +"perform_batch_id INTEGER," 
//				  +"time INTEGER,"
//				  +"load_type INTEGER,"
//				  +"avg_cost_time INTEGER,"
//				  +"request_times INTEGER,"
//				  +"success_times INTEGER,"
//				  +"failed_times INTEGER)";
		String[] columns={"time","perform_batch_id","load_type","avg_cost_time","request_times","success_times","failed_times","tps"};
		Object[] values={record.getTime(),record.getPerformBatchId(),record.getLoadType(),record.getAvgCostTime(),record.getCurrentRequests(),record.getSuccessRequests(),record.getFailedRequeests(),record.getThroughtput()};
		BizDBUtils.insertBySqlAndParamAndTable(columns,values,"ts_requests_tp_perform");
	}
	public static Map<String,Object> selectLoadBatchByDbAndUrl(String dbClass, String dbUrl) {
		Connection conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<Map<String,Object>> rsh=new ResultSetHandler<Map<String,Object>>() {
			@Override
			public Map<String,Object> handle(ResultSet rs) throws SQLException {
				Map<String,Object> map=new HashMap<String, Object>();
				while (rs.next()) {
					ResultSetMetaData metaData = rs.getMetaData();
					int columns = metaData.getColumnCount();
					for(int i=0;i<columns;i++){
						String columnName = metaData.getColumnName(i+1);
//						System.out.println(columnName);
						Object obj=rs.getObject(columnName);
						map.put(columnName, obj);
					}
					return map;
				}
				return null;
			}
		};
		String queryDeviceSql="select * from ts_load_batch where db_url=? and data_status=1 and target_db=? order by id desc limit 1";
		Map<String,Object> map=null;
		try {
			Object[] params={dbUrl,dbClass};
			map=runner.query(conn,queryDeviceSql, rsh,params);
		} catch (SQLException e) {
			e.printStackTrace();
		}
//		System.out.println(map);
		return map;
	}
	public static List<Map<String,Object>> selectListBySqlAndParam(String sql,Object... params) {
		//TODO
		Connection conn=getConnection();
		QueryRunner runner=new QueryRunner();
		ResultSetHandler<List<Map<String,Object>>> rsh=new ResultSetHandler<List<Map<String,Object>>>() {
			@Override
			public List<Map<String,Object>> handle(ResultSet rs) throws SQLException {
				List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
				while (rs.next()) {
					Map<String,Object> map=new HashMap<String, Object>();
					ResultSetMetaData metaData = rs.getMetaData();
					int columns = metaData.getColumnCount();
					for(int i=0;i<columns;i++){
//						String columnName = metaData.getColumnName(i+1);
						String columnName = metaData.getColumnLabel(i+1);
						Object obj=rs.getObject(columnName);
						map.put(columnName, obj);
					}
					list.add(map);
				}
				return list;
			}
		};
//		String queryDeviceSql="select * from ts_device_info where load_batch_id=?";
		List<Map<String,Object>> list=null;
		try {
//			Object[] params={Constants.LOAD_BATCH_ID};
			list=runner.query(conn,sql, rsh,params);
		} catch (SQLException e) {
			e.printStackTrace();
//			System.exit(0);
		}
		return list;
	}
	public static void initSensorCodes() {
		// TODO Auto-generated method stub
		
	}
	public static void initShiftTime() {
		// TODO Auto-generated method stub
		
	}
	public static void initSensorFunction() {
		// TODO Auto-generated method stub
		
	}
	public static void main(String[] args) {
		selectLoadBatchByDbAndUrl("cn.edu.ruc.db.InfluxDB", "http://127.0.0.1:8086");
	}
	public static void insertThroughputRecord(ThroughputRecord record) {
		//TODO
		String[] columns={"perform_batch_id","db_type","sum_success_count","cost_time","ops"};
		Object[] values={record.getPerformBathchId(),Constants.DB_CLASS,record.getSumTimes(),(long)record.getCostTime(),record.getSumTimes()/record.getCostTime()};
		long ttid = BizDBUtils.insertBySqlAndParamAndTable(columns,values,"ts_throughput");
		LoadTypeEnum[] enums = LoadTypeEnum.values();
		for(LoadTypeEnum loadType:enums){
			insertThroughputRecordDetail(record,loadType, ttid);
		}
	}
	private static void insertThroughputRecordDetail(ThroughputRecord record,LoadTypeEnum type,long ttid) {
		String[] columns={"tt_id","operate_type","operate_times","failed_times","avg_latency","min_latency","max_latency"};
		RequestTimeout timeout = record.getTimeoutByLoadType(type);
		if(timeout==null){
			return;
		}
		Object[] values={ttid,timeout.getRequestType(),timeout.getSuccessTimes(),timeout.getFailedTimes(),timeout.getAvgTimeout()/1000,timeout.getMinTimeout()/1000,timeout.getMaxTimeout()/1000};
		BizDBUtils.insertBySqlAndParamAndTable(columns,values,"ts_throughput_detail");
	}
}


