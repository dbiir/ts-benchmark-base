package cn.edu.ruc.base;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
/**
 * 时间序列数据包 
 * 一个设备的某一个时间点的所有数据
 * @author fasape
 *
 */
public class TsPackage implements Serializable{
	private static final long serialVersionUID = 1L;
	private String deviceCode;
	private long timestamp;
	private Map<String,Object> valueMap=new HashMap<String,Object>();
	public String getDeviceCode() {
		return deviceCode;
	}
	public void setDeviceCode(String deviceCode) {
		this.deviceCode = deviceCode;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public Map<String, Object> getValueMap() {
		return valueMap;
	}
	public void setValueMap(Map<String, Object> valueMap) {
		this.valueMap = valueMap;
	}
	public void appendValue(String sensorCode,Object value) {
		valueMap.put(sensorCode, value);
	}
	public Set<String> getSensorCodes(){
		return valueMap.keySet();
	}
	public Object getValue(String sensorCode) {
		return valueMap.get(sensorCode);
	}
}
