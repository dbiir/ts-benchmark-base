package cn.edu.ruc.biz.model;

import java.io.Serializable;
public class RequestTimeout implements Serializable{
	private static final long serialVersionUID = 1L;
	private int requestType;//LoadTypeEnum
	private long costTime=0;
	private long successTimes=0;
	private long failedTimes=0;
	private long maxTimeout=0;
	private long minTimeout=Long.MAX_VALUE;
	public synchronized void  addSuccessTimes(){
		successTimes++;
	}
	public synchronized void addFailedTimes(){
		failedTimes++;
	}
	public synchronized void addCostTime(long costTime){
		this.costTime+=costTime;
		if(costTime>maxTimeout){
			this.maxTimeout=costTime;
		}
		if(costTime<minTimeout){
			this.minTimeout=costTime;
		}
	}
	public long getAvgTimeout(){
		if(successTimes==0){
			return 0;
		}else{
			return costTime/successTimes;
		}
	}
	public RequestTimeout(int requestType) {
		super();
		this.requestType = requestType;
	}
	public int getRequestType() {
		return requestType;
	}
	public long getCostTime() {
		return costTime;
	}
	public long getSuccessTimes() {
		return successTimes;
	}
	public long getFailedTimes() {
		return failedTimes;
	}
	public long getMaxTimeout() {
		return maxTimeout;
	}
	public long getMinTimeout() {
		if(successTimes==0){
			return 0;
		}
		return minTimeout;
	}
}

