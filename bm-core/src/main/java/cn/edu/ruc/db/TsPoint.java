package cn.edu.ruc.db;


/**
 * 时序测点数据对象
 * @author sxg
 */
public class TsPoint {
	/**
	 * 测点所属设备编号
	 */
	private String deviceCode;
	/**
	 * 测点所属传感器编号
	 */
	private String sensorCode;
	/**
	 * 值
	 */
	private Object value;
	/**
	 * 数据时间戳
	 * 单位为毫秒
	 */
	private long timestamp;
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public String getDeviceCode() {
		return deviceCode;
	}
	public void setDeviceCode(String deviceCode) {
		this.deviceCode = deviceCode;
	}
	public String getSensorCode() {
		return sensorCode;
	}
	public void setSensorCode(String sensorCode) {
		this.sensorCode = sensorCode;
	}
	/**
	 * 跟另一个数据点判断是否是同一个设备同一个时间点的数据
	 * @param point 要对比的数据点
	 * @return true:是;false:否
	 */
	public boolean isSameDeviceAndTime(TsPoint point){
		if(point==null){
			return false;
		}
		if(point.getTimestamp()==getTimestamp()&&point.getDeviceCode().contains(getDeviceCode())){
			return true;
		}else{
			return false;
		}
	}
}

