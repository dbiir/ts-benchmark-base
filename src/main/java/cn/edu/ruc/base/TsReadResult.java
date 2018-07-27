package cn.edu.ruc.base;

import java.io.Serializable;
/**
 * 读结果集对象
 * @author fasape
 *
 */
public class TsReadResult implements Serializable{
	private static final long serialVersionUID = 1L;
	private String batchCode;//批次号
	private Integer tps;//吞吐量
	private double successRatio=0;//请求成功率
	
	
	private Integer meanTimeout;//
	private Integer maxTimeout;//
	private Integer minTimeout;//
	private Integer fiftyTimeout;//中位timeout us
	private Integer ninty5Timeout;//第95%timeout us
	
	private Long startTime;//开始时间
	private Long endTime;//结束时间
	private Long sumRequests;//总请求数目
	public Integer getTps() {
		return tps;
	}
	public void setTps(Integer tps) {
		this.tps = tps;
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
	public String getBatchCode() {
		return batchCode;
	}
	public void setBatchCode(String batchCode) {
		this.batchCode = batchCode;
	}
	public Long getSumRequests() {
		return sumRequests;
	}
	public void setSumRequests(Long sumRequests) {
		this.sumRequests = sumRequests;
	}
	public double getSuccessRatio() {
		return successRatio;
	}
	public void setSuccessRatio(double successRatio) {
		this.successRatio = successRatio;
	}
}
