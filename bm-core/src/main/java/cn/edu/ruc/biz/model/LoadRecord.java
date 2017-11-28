package cn.edu.ruc.biz.model;

import java.io.Serializable;

import cn.edu.ruc.biz.Constants;

public class LoadRecord implements Serializable {
	private static final long serialVersionUID = 1L;
	private Long id;
	private Long loadTime = System.currentTimeMillis();
	private Long batchId =Constants.LOAD_BATCH_ID;
	private Integer loadPoints;
	private Integer loadSize;
	private Long loadCostTime;
	private Integer pps;
	private double sps;//MB/S
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public Long getLoadTime() {
		return loadTime;
	}
	public void setLoadTime(Long loadTime) {
		this.loadTime = loadTime;
	}
	public Long getBatchId() {
		return batchId;
	}
	public void setBatchId(Long batchId) {
		this.batchId = batchId;
	}
	public Integer getLoadPoints() {
		return loadPoints;
	}
	public void setLoadPoints(Integer loadPoints) {
		this.loadPoints = loadPoints;
	}
	public Long getLoadCostTime() {
		return loadCostTime;
	}
	public void setLoadCostTime(Long loadCostTime) {
		this.loadCostTime = loadCostTime;
	}
	public Integer getPps() {
		return pps;
	}
	public void setPps(Integer pps) {
		this.pps = pps;
	}
	public Integer getLoadSize() {
		return loadSize;
	}
	public void setLoadSize(Integer loadSize) {
		this.loadSize = loadSize;
	}
	public double getSps() {
		return sps;
	}
	public void setSps(double sps) {
		this.sps = sps;
	}
}

