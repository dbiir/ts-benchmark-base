package cn.edu.ruc.base;

import cn.edu.ruc.cmd.CfgName;

/**
 * ts数据库 数据源配置
 * @author fasape
 *
 */
public class TsDataSource {
//	@CfgName(name="DB_DRIVER_CLASS")
	private String driverClass;
	@CfgName(name="DB_IP")
	private String ip;
	@CfgName(name="DB_PORT")
	private String port;
	@CfgName(name="DB_PASSWD")
	private String passwd;
	@CfgName(name="DB_USER")
	private String user;
	@CfgName(name="BATCH_CODE")
	private String batchCode;
	@CfgName(name="DB_TYPE")
	private String dbType;
	public String getDriverClass() {
		return driverClass;
	}
	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getPasswd() {
		return passwd;
	}
	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	@Override
	public String toString() {
		return "TsDataSource [driverClass=" + driverClass + ", ip=" + ip + ", port=" + port + ", passwd=" + passwd
				+ ", user=" + user + "]";
	}
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
	}
	public String getDbType() {
		return dbType;
	}
	public void setDbType(String dbType) {
		this.dbType = dbType;
	}
	public String getBatchCode() {
		return batchCode;
	}
	public void setBatchCode(String batchCode) {
		this.batchCode = batchCode;
	}
}
