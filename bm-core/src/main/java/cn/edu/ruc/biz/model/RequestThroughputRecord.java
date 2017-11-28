package cn.edu.ruc.biz.model;

import java.io.Serializable;

import cn.edu.ruc.biz.Constants;
/**
 * 请求吞吐量测试
 * @author sxg
 */
public class RequestThroughputRecord implements Serializable{
	private static final long serialVersionUID = 1L;
	private long id;
	private long performBatchId=Constants.PERFORM_BATCH_ID;
	private long time=System.currentTimeMillis();
	private int loadType;
	private double throughtput;
	private double avgCostTime;
	private long currentRequests;
	private long successRequests;
	private long failedRequeests;
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	public long getId() {
		return id;
	}
	public long getPerformBatchId() {
		return performBatchId;
	}
	public long getTime() {
		return time;
	}
	public double getThroughtput() {
		return throughtput;
	}
	public double getAvgCostTime() {
		return avgCostTime;
	}
	public long getCurrentRequests() {
		return currentRequests;
	}
	public long getSuccessRequests() {
		return successRequests;
	}
	public long getFailedRequeests() {
		return failedRequeests;
	}
	public RequestThroughputRecord(int loadType,double throughtput, double avgCostTime,
			long currentRequests, long successRequests, long failedRequeests) {
		super();
		this.loadType=loadType;
		this.throughtput = throughtput;
		this.avgCostTime = avgCostTime;
		this.currentRequests = currentRequests;
		this.successRequests = successRequests;
		this.failedRequeests = failedRequeests;
	}
	@Override
	public String toString() {
		return "RequestThroughputRecord [throughtput=" + throughtput
				+ ", avgCostTime=" + avgCostTime + ", currentRequests="
				+ currentRequests + ", successRequests=" + successRequests
				+ ", failedRequeests=" + failedRequeests + "]";
	}
	public int getLoadType() {
		return loadType;
	}
}

