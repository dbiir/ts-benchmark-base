package cn.edu.ruc.base;

import java.io.Serializable;

public class TsWriteResult implements Serializable{
	private static final long serialVersionUID = 1L;
	private String batchCode;//批次号
	private Integer pps;//吞吐量
	
	private Integer meanTimeout;// kpoints
	private Integer maxTimeout;//
	private Integer minTimeout;//
	private Integer fiftyTimeout;//中位timeout us/kpoints
	private Integer ninty5Timeout;//第95%timeout us/kpoints
	
	private Long startTime;//开始时间
	private Long endTime;//结束时间
	private Long sumPoints;//总请求数目
	public String getBatchCode() {
		return batchCode;
	}
	public void setBatchCode(String batchCode) {
		this.batchCode = batchCode;
	}
	public Integer getPps() {
		return pps;
	}
	public void setPps(Integer pps) {
		this.pps = pps;
	}
	public Integer getMeanTimeout() {
		return meanTimeout;
	}
	public void setMeanTimeout(Integer meanTimeout) {
		this.meanTimeout = meanTimeout;
	}
	public Integer getMaxTimeout() {
		return maxTimeout;
	}
	public void setMaxTimeout(Integer maxTimeout) {
		this.maxTimeout = maxTimeout;
	}
	public Integer getMinTimeout() {
		return minTimeout;
	}
	public void setMinTimeout(Integer minTimeout) {
		this.minTimeout = minTimeout;
	}
	public Integer getFiftyTimeout() {
		return fiftyTimeout;
	}
	public void setFiftyTimeout(Integer fiftyTimeout) {
		this.fiftyTimeout = fiftyTimeout;
	}
	public Integer getNinty5Timeout() {
		return ninty5Timeout;
	}
	public void setNinty5Timeout(Integer ninty5Timeout) {
		this.ninty5Timeout = ninty5Timeout;
	}
	public Long getStartTime() {
		return startTime;
	}
	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}
	public Long getEndTime() {
		return endTime;
	}
	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}
	public Long getSumPoints() {
		return sumPoints;
	}
	public void setSumPoints(Long sumPoints) {
		this.sumPoints = sumPoints;
	}
}
