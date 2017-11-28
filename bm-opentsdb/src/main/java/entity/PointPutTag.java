package entity;

import cn.edu.ruc.db.TsPoint;

/**
@author Wang-Practice makes perfect
@version 2017年7月19日
类说明
*/
public class PointPutTag {
	private String host;
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getDc() {
		return dc;
	}

	public void setDc(String dc) {
		this.dc = dc;
	}

	private String dc;

	public PointPutTag(TsPoint tp) {
	
		this.host = tp.getDeviceCode();
		this.dc = tp.getSensorCode();
	}
	
}
