package cn.edu.ruc.db;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import cn.edu.ruc.biz.Core;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * Cassandra处理数据库类
 * 
 * @author RUC
 */
public class CassandraDB extends DBBase {
	private static final String CASSADRA_URL_PROPERTY = "Cassandra.url";
	private static String CASSADRA_URL = "";
	private static String KEY_SPACE_NAME = "ruc_perform";
	private static String TABLE_NAME = "point";

	public static void main(String[] args) throws Exception {
		 Core.main(args);
		// DBBase dbBase=new CassandraDB();
		// dbBase.init();
		// List<TsPoint> points=new ArrayList<TsPoint>();
		// for(int i=0;i<10;i++){
		// TsPoint point=new TsPoint();
		// point.setDeviceCode("d"+"_"+i);
		// point.setSensorCode("s"+"_"+i);
		// point.setTimestamp(System.currentTimeMillis());
		// point.setValue(System.currentTimeMillis());
		// try {
		// Thread.currentThread().sleep(1L);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// points.add(point);
		// }
		// dbBase.insertMulti(points);

		CassandraDB db = new CassandraDB();
		db.init();
		TsPoint point = new TsPoint();
		point.setDeviceCode("d_fh_7");
		point.setSensorCode("s_gaz_5");

		// db.selectByDevice(point, TSUtils.getDateByDateStr("2014-01-01
		// 00:00:00"),TSUtils.getDateByDateStr("2018-08-22 00:00:00"));
		// db.updatePoints
		// db.selectByDeviceAndSensor(point,TSUtils.getDateByDateStr("2014-01-01
		// 00:00:00") ,TSUtils.getDateByDateStr("2018-08-22 00:00:00"));
		// db.selectByDeviceAndSensor(point,19.111111,-20.111111111,TSUtils.getDateByDateStr("2014-01-01
		// 00:00:00") ,TSUtils.getDateByDateStr("2018-08-22 00:00:00"));
		// db.selectMaxByDeviceAndSensor("d_fh_1", "s_gaz_1",
		// TSUtils.getDateByDateStr("2014-01-01
		// 00:00:00"),TSUtils.getDateByDateStr("2018-08-22 00:00:00"));
		// db.selectMinByDeviceAndSensor("d_fh_1", "s_gaz_1",
		// TSUtils.getDateByDateStr("2014-01-01 00:00:00"),
		// TSUtils.getDateByDateStr("2018-08-22 00:00:00"));
		// db.selectAvgByDeviceAndSensor("d_fh_1", "s_gaz_1",
		// TSUtils.getDateByDateStr("2014-01-01
		// 00:00:00"),TSUtils.getDateByDateStr("2018-08-22 00:00:00"));
		// db.selectCountByDeviceAndSensor("d_fh_1", "s_gaz_1",
		// TSUtils.getDateByDateStr("2014-01-01 00:00:00"),
		// TSUtils.getDateByDateStr("2018-08-22 00:00:00"));
	}

	@Override
	public void init() {
		super.init();
		Properties properties = getProperties();
		CASSADRA_URL = properties.getProperty(CASSADRA_URL_PROPERTY, "127.0.0.1");
		SessionManager.url=CASSADRA_URL;
		SessionManager.keyspaceName=KEY_SPACE_NAME;
		Cluster cluster = null;
		try {
//			cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
//			Session connect = cluster.connect();
			Session connect = SessionManager.getRootSession();
			connect.execute("CREATE KEYSPACE  IF NOT EXISTS " + KEY_SPACE_NAME
					+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}");//FIXME 副本数
//			connect.close();
//			Session session = cluster.connect(KEY_SPACE_NAME);
			Session session = SessionManager.getSession();
			String createTableCql = "CREATE TABLE  IF NOT EXISTS " + TABLE_NAME
					+ "(timestamp timestamp,device_code text,sensor_code text,value double,primary key(timestamp,device_code,sensor_code)) "
					+ "WITH comment='wind test records'  AND read_repair_chance = 1.0";
			ResultSet rs = session.execute(createTableCql);
			String createIndexCql = "CREATE INDEX IF NOT EXISTS value_index ON " + TABLE_NAME + "(value)";
			session.execute(createIndexCql);
		} finally {
			if (cluster != null)
				cluster.close();
		}
	}

	@Override
	public Status insertMulti(List<TsPoint> points) {
		long costTime = 0L;
		if (points != null) {
			Cluster cluster = null;
			try {
//				cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
//				Session session = cluster.connect(KEY_SPACE_NAME);
				Session session = SessionManager.getSession();
				BatchStatement batch = new BatchStatement();
				PreparedStatement ps = session.prepare(
						"INSERT INTO " + TABLE_NAME + "(timestamp,device_code,sensor_code,value) VALUES(?,?,?,?)");
				for (TsPoint point : points) {
					BoundStatement bs = ps.bind(new Date(point.getTimestamp()), point.getDeviceCode(),
							point.getSensorCode(), Double.parseDouble(point.getValue().toString()));
					batch.add(bs);
				}
				long startTime = System.nanoTime();
				session.execute(batch);
				long endTime = System.nanoTime();
				costTime = endTime - startTime;
				batch.clear();
//				session.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (cluster != null)
					cluster.close();
			}
		}
//		System.out.println("costTime=" + costTime);
		return Status.OK(costTime);
	}

	@Override
	public Status selectByDevice(TsPoint point, Date startTime, Date endTime) {
		long costTime = 0L;
		Cluster cluster = null;
		try {
//			cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
//			Session session = cluster.connect(KEY_SPACE_NAME);
			Session session = SessionManager.getSession();
			String selectCql = "SELECT * FROM point WHERE device_code='" + point.getDeviceCode() + "' and timestamp>="
					+ startTime.getTime() + " and timestamp<=" + endTime.getTime() + " ALLOW FILTERING";
//			System.out.println(selectCql);
			long startTime1 = System.nanoTime();
			ResultSet rs = session.execute(selectCql);
			long endTime1 = System.nanoTime();
			costTime = endTime1 - startTime1;
		} finally {
			if (cluster != null)
				cluster.close();
		}
//		System.out.println("此次查询消耗时间[" + costTime / 1000 + "]s");
		return Status.OK(costTime);
	}

	@Override
	public Status selectDayMaxByDevice(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectDayMinByDevice(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectDayAvgByDevice(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectHourMaxByDevice(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectHourMinByDevice(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectHourAvgByDevice(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectMinuteMaxByDeviceAndSensor(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectMinuteMinByDeviceAndSensor(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectMinuteAvgByDeviceAndSensor(TsPoint point, Date startTime, Date endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status updatePoints(List<TsPoint> points) {
		long costTime = 0L;
		if (points != null) {
			Cluster cluster = null;
			try {
//				cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
//				Session session = cluster.connect(KEY_SPACE_NAME);
				Session session = SessionManager.getSession();
				BatchStatement batch = new BatchStatement();
				PreparedStatement ps = session.prepare(
						"UPDATE " + TABLE_NAME + " SET value=? WHERE timestamp=? and device_code=? and sensor_code=?");
				for (TsPoint point : points) {
					BoundStatement bs = ps.bind(Double.parseDouble(point.getValue().toString()),new Date(point.getTimestamp()), point.getDeviceCode(),
							point.getSensorCode());
					batch.add(bs);
				}
				long startTime = System.nanoTime();
				session.execute(batch);
				long endTime = System.nanoTime();
				costTime = endTime - startTime;
				batch.clear();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (cluster != null)
					cluster.close();
			}
		}
//		System.out.println("此次更新消耗时间[" + costTime / 1000 + "]s");
		return Status.OK(costTime);
	}

	@Override
	public Status deletePoints(Date date) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Date startTime, Date endTime) {
		long costTime = 0L;
		Cluster cluster = null;
		try {
//			cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
//			Session session = cluster.connect(KEY_SPACE_NAME);
			Session session = SessionManager.getSession();
			String selectCql = "SELECT * FROM point WHERE device_code='" + point.getDeviceCode() + "' and sensor_code='"
					+ point.getSensorCode() + "' and timestamp>=" + startTime.getTime() + " and timestamp<="
					+ endTime.getTime() + " ALLOW FILTERING";
//			System.out.println(selectCql);
			long startTime1 = System.nanoTime();
			ResultSet rs = session.execute(selectCql);
			long endTime1 = System.nanoTime();
			costTime = endTime1 - startTime1;
		} finally {
			if (cluster != null)
				cluster.close();
		}
//		System.out.println("此次查询消耗时间[" + costTime / 1000 + "]s");
		return Status.OK(costTime);
	}

	@Override
	public Status selectByDeviceAndSensor(TsPoint point, Double max, Double min, Date startTime, Date endTime) {
		long costTime = 0L;
		Cluster cluster = null;
		try {
			cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
			Session session = cluster.connect(KEY_SPACE_NAME);
			String createIndexCql = "CREATE INDEX IF NOT EXISTS value_index ON " + TABLE_NAME + "(value)";
//			System.out.println(createIndexCql);
			long startTime1 = System.nanoTime();
			session.execute(createIndexCql);
			String selectCql = "SELECT * FROM point WHERE device_code='" + point.getDeviceCode() + "' and sensor_code='"
					+ point.getSensorCode() + "' and value<" + max + " and value>" + min + " and timestamp>="
					+ startTime.getTime() + " and timestamp<=" + endTime.getTime() + " ALLOW FILTERING";
//			System.out.println(selectCql);
			ResultSet rs = session.execute(selectCql);
			long endTime1 = System.nanoTime();
			costTime = endTime1 - startTime1;
		} finally {
			if (cluster != null)
				cluster.close();
		}
//		System.out.println("此次查询消耗时间[" + costTime / 1000 + "]s");
		return Status.OK(costTime);
	}

	@Override
	public Status selectMaxByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		long costTime = 0L;
		Cluster cluster = null;
		try {
//			cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
//			Session session = cluster.connect(KEY_SPACE_NAME);
			Session session = SessionManager.getSession();
			String selectCql = "SELECT MAX(value) FROM point WHERE device_code='" + deviceCode + "' and sensor_code='"
					+ sensorCode + "' and timestamp>=" + startTime.getTime() + " and timestamp<=" + endTime.getTime()
					+ " ALLOW FILTERING";
			long startTime1 = System.nanoTime();
//			System.out.println("aaa");
			ResultSet rs = session.execute(selectCql);
//			System.out.println("bbb");
			long endTime1 = System.nanoTime();
			costTime = endTime1 - startTime1;
		} finally {
			if (cluster != null)
				cluster.close();
		}
//		System.out.println("此次查询消耗时间[" + costTime / 1000 + "]s");
		return Status.OK(costTime);
	}

	@Override
	public Status selectMinByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		long costTime = 0L;
		Cluster cluster = null;
		try {
			cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
			Session session = cluster.connect(KEY_SPACE_NAME);
			String selectCql = "SELECT MIN(value) FROM point WHERE device_code='" + deviceCode + "' and sensor_code='"
					+ sensorCode + "' and timestamp>=" + startTime.getTime() + " and timestamp<=" + endTime.getTime()
					+ " ALLOW FILTERING";
//			System.out.println(selectCql);
			long startTime1 = System.nanoTime();
			ResultSet rs = session.execute(selectCql);
			long endTime1 = System.nanoTime();
			costTime = endTime1 - startTime1;
		} finally {
			if (cluster != null)
				cluster.close();
		}
//		System.out.println("此次查询消耗时间[" + costTime / 1000 + "]s");
		return Status.OK(costTime);
	}

	@Override
	public Status selectAvgByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		long costTime = 0L;
		Cluster cluster = null;
		try {
			cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
			Session session = cluster.connect(KEY_SPACE_NAME);
			String selectCql = "SELECT AVG(value) FROM point WHERE device_code='" + deviceCode + "' and sensor_code='"
					+ sensorCode + "' and timestamp>=" + startTime.getTime() + " and timestamp<=" + endTime.getTime()
					+ " ALLOW FILTERING";
//			System.out.println(selectCql);
			long startTime1 = System.nanoTime();
			ResultSet rs = session.execute(selectCql);
			long endTime1 = System.nanoTime();
			costTime = endTime1 - startTime1;
		} finally {
			if (cluster != null)
				cluster.close();
		}
//		System.out.println("此次查询消耗时间[" + costTime / 1000 + "]s");
		return Status.OK(costTime);
	}

	@Override
	public Status selectCountByDeviceAndSensor(String deviceCode, String sensorCode, Date startTime, Date endTime) {
		long costTime = 0L;
		Cluster cluster = null;
		try {
			cluster = Cluster.builder().addContactPoint(CASSADRA_URL).build();
			Session session = cluster.connect(KEY_SPACE_NAME);
			String selectCql = "SELECT COUNT(*) FROM point WHERE device_code='" + deviceCode + "' and sensor_code='"
					+ sensorCode + "' and timestamp>=" + startTime.getTime() + " and timestamp<=" + endTime.getTime()
					+ " ALLOW FILTERING";
//			System.out.println(selectCql);
			long startTime1 = System.nanoTime();
			ResultSet rs = session.execute(selectCql);
			long endTime1 = System.nanoTime();
			costTime = endTime1 - startTime1;
		} finally {
			if (cluster != null)
				cluster.close();
		}
//		System.out.println("此次查询消耗时间[" + costTime / 1000 + "]s");
		return Status.OK(costTime);
	}

	@Override
	public String getDBUrl() {
		return CASSADRA_URL;
	}
	@Override
	public void cleanup() {
		super.cleanup();
		SessionManager.closeCluster();
	}
}
