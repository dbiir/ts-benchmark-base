package cn.edu.ruc.base;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
/**
 * 
 * @author fasape
 *
 */
public class TsQuery implements Serializable{
	private static final long serialVersionUID = 1L;
	private int queryType=1;//查询类型 1:简单查询 2:分析查询  3:多设备聚合
	private String deviceName;//设备名称 必传 
	private String sensorName;//传感器名称  必传
	private Long startTimestamp;
	private Long endTimestamp;
	private Double sensorLtValue;//传感器值小于某个值
	private Double sensorGtValue;//传感器值大于某个值
	private Integer groupByUnit;//聚合查询 组类别 1:s 2:min 3:hour 4:day 5:month 6:year
	private Integer aggreType;//聚合查询类型 1 max 2 min 3 avg 4 count
	private List<String> devices=new ArrayList<String>();
	public List<String> getDevices() {
		return devices;
	}
	public void setDevices(List<String> devices) {
		this.devices = devices;
	}
	public int getQueryType() {
		return queryType;
	}
	public void setQueryType(int queryType) {
		this.queryType = queryType;
	}
	public String getDeviceName() {
		return deviceName;
	}
	public void setDeviceName(String deviceName) {
		this.deviceName = deviceName;
	}
	public String getSensorName() {
		return sensorName;
	}
	public void setSensorName(String sensorName) {
		this.sensorName = sensorName;
	}
	public Long getStartTimestamp() {
		return startTimestamp;
	}
	public void setStartTimestamp(Long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}
	public Long getEndTimestamp() {
		return endTimestamp;
	}
	public void setEndTimestamp(Long endTimestamp) {
		this.endTimestamp = endTimestamp;
	}
	public Double getSensorLtValue() {
		return sensorLtValue;
	}
	public void setSensorLtValue(Double sensorLtValue) {
		this.sensorLtValue = sensorLtValue;
	}
	public Double getSensorGtValue() {
		return sensorGtValue;
	}
	public void setSensorGtValue(Double sensorGtValue) {
		this.sensorGtValue = sensorGtValue;
	}
	public Integer getGroupByUnit() {
		return groupByUnit;
	}
	public void setGroupByUnit(int groupByUnit) {
		this.groupByUnit = groupByUnit;
	}
	public Integer getAggreType() {
		return aggreType;
	}
	public void setAggreType(int aggreType) {
		this.aggreType = aggreType;
	}
	@Override
	public String toString() {
		return "TsQuery [queryType=" + queryType + ", deviceName=" + deviceName + ", sensorName=" + sensorName
				+ ", startTimestamp=" + startTimestamp + ", endTimestamp=" + endTimestamp + ", sensorLtValue="
				+ sensorLtValue + ", sensorGtValue=" + sensorGtValue + ", groupByUnit=" + groupByUnit + ", aggreType="
				+ aggreType + "]";
	}
	public static void main(String[] args) {
		System.out.println(new TsQuery());
	}
}
