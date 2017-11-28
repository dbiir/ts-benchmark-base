package entity;

import cn.edu.ruc.db.TsPoint;

/**
@author Wang-Practice makes perfect
@version 2017年7月19日
类说明
*/
public class PointPut  {
//	public static final long serialVersionUID = 1L;
	public String metric="time.series.perform";
	public String timestamp;
	public Object value;
	public PointPutTag tags;
		
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public Object getValue() {
		return  value;
	}
	//public void setValue(Object object) {
	public void setValue(Object object) {
		this.value = object;
	}
	
	public PointPutTag getTags() {
		return tags;
	}
	public void setTags(PointPutTag tags) {
		this.tags = tags;
	}
	@Override
	public String toString() {
		return "PointPut [metric=" + metric + ", timestamp=" + timestamp + ", value=" + value + ", tags=" + tags + "]";
	}
	
}
