package cn.edu.ruc.biz.model;

import java.io.Serializable;

import cn.edu.ruc.biz.Constants;

public class WriteRecord implements Serializable{
	private static final long serialVersionUID = 1L;
	private Long id;
	private Long batchId=Constants.PERFORM_BATCH_ID;
	private Long writeTime=System.currentTimeMillis();
	private Integer targetDnPs;
	private Integer targetPointPs;
	private Integer realPointPs;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public Long getBatchId() {
		return batchId;
	}
	public void setBatchId(Long batchId) {
		this.batchId = batchId;
	}
	public Long getWriteTime() {
		return writeTime;
	}
	public void setWriteTime(Long writeTime) {
		this.writeTime = writeTime;
	}
	public Integer getTargetDnPs() {
		return targetDnPs;
	}
	public void setTargetDnPs(Integer targetDnPs) {
		this.targetDnPs = targetDnPs;
	}
	@Override
	public String toString() {
		return "WriteRecord [id=" + id + ", batchId=" + batchId
				+ ", writeTime=" + writeTime + ", targetDnPs=" + targetDnPs
				+ ", targetPointPs=" + targetPointPs + ", realPointPs="
				+ realPointPs + "]";
	}
	public Integer getTargetPointPs() {
		return targetPointPs;
	}
	public void setTargetPointPs(Integer targetPointPs) {
		this.targetPointPs = targetPointPs;
	}
	public Integer getRealPointPs() {
		return realPointPs;
	}
	public void setRealPointPs(Integer realPointPs) {
		this.realPointPs = realPointPs;
	}
}

