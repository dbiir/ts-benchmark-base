package cn.edu.ruc.biz.model;

import java.io.Serializable;

import cn.edu.ruc.biz.Constants;
import cn.edu.ruc.biz.Core;
import cn.edu.ruc.enums.LoadTypeEnum;

/**
 * 性能测试(指标为吞吐量) 记录 
 * 记录各类请求总消耗时间(ns)，请求成功次数(long)，失败次数(long)，所有请求总次数(long)
 */
public class ThroughputRecord implements Serializable{
	private static final long serialVersionUID = 1L;
	private long id;
	private long performBathchId=Constants.PERFORM_BATCH_ID;
	private long sumTimes=0;//总请求次数
	
	private RequestTimeout writeTimeout=new RequestTimeout(LoadTypeEnum.WRITE.getId());
	private RequestTimeout randomInsertTimeout=new RequestTimeout(LoadTypeEnum.RANDOM_INSERT.getId());
	private RequestTimeout simpleReadTimeout=new RequestTimeout(LoadTypeEnum.SIMPLE_READ.getId());
	private RequestTimeout analysisReadTimeout=new RequestTimeout(LoadTypeEnum.AGGRA_READ.getId());
	private RequestTimeout updateTimeout=new RequestTimeout(LoadTypeEnum.UPDATE.getId());
	
	private double costTime=0;//消耗时间  us
	
	/**
	 * @param loadType
	 * @param timeout
	 */
	public  void  addSuccessTimes(Integer loadType,long timeout){
		if(LoadTypeEnum.WRITE.getId().equals(loadType)){
			writeTimeout.addSuccessTimes();
			writeTimeout.addCostTime(timeout);
		}
		if(LoadTypeEnum.RANDOM_INSERT.getId().equals(loadType)){
			randomInsertTimeout.addSuccessTimes();
			randomInsertTimeout.addCostTime(timeout);
		}
		if(LoadTypeEnum.UPDATE.getId().equals(loadType)){
			updateTimeout.addSuccessTimes();
			updateTimeout.addCostTime(timeout);
		}
		if(LoadTypeEnum.SIMPLE_READ.getId().equals(loadType)){
			simpleReadTimeout.addSuccessTimes();
			simpleReadTimeout.addCostTime(timeout);
		}
		if(LoadTypeEnum.AGGRA_READ.getId().equals(loadType)){
			analysisReadTimeout.addSuccessTimes();
			analysisReadTimeout.addCostTime(timeout);
		}
	}
	public  void  addFailedTimes(Integer loadType){
		if(LoadTypeEnum.WRITE.getId().equals(loadType)){
			writeTimeout.addFailedTimes();
		}
		if(LoadTypeEnum.RANDOM_INSERT.getId().equals(loadType)){
			randomInsertTimeout.addFailedTimes();
		}
		if(LoadTypeEnum.UPDATE.getId().equals(loadType)){
			updateTimeout.addFailedTimes();
		}
		if(LoadTypeEnum.SIMPLE_READ.getId().equals(loadType)){
			simpleReadTimeout.addFailedTimes();
		}
		if(LoadTypeEnum.AGGRA_READ.getId().equals(loadType)){
			analysisReadTimeout.addFailedTimes();
		}
	}
	public long getId() {
		return id;
	}
	public long getPerformBathchId() {
		return performBathchId;
	}
	public long getSumTimes() {
		return getAnalysisReadTimeout().getSuccessTimes()+getRandomInsertTimeout().getSuccessTimes()+getSimpleReadTimeout().getSuccessTimes()+getUpdateTimeout().getSuccessTimes()+getWriteTimeout().getSuccessTimes();
	}
	public RequestTimeout getWriteTimeout() {
		return writeTimeout;
	}
	public RequestTimeout getRandomInsertTimeout() {
		return randomInsertTimeout;
	}
	public RequestTimeout getSimpleReadTimeout() {
		return simpleReadTimeout;
	}
	public RequestTimeout getAnalysisReadTimeout() {
		return analysisReadTimeout;
	}
	public RequestTimeout getUpdateTimeout() {
		return updateTimeout;
	}
	public void printlnTimeout(LoadTypeEnum loadType){
		RequestTimeout timeout = getTimeoutByLoadType(loadType);
		Core.println("["+loadType.getDesc()+"],Operations, "+timeout.getSuccessTimes());
		Core.println("["+loadType.getDesc()+"],Failed, "+timeout.getFailedTimes());
		Core.println("["+loadType.getDesc()+"],AverageLatency(us),"+timeout.getAvgTimeout()/1000);
		Core.println("["+loadType.getDesc()+"], MinLatency(us),"+timeout.getMinTimeout()/1000);
		Core.println("["+loadType.getDesc()+"], MaxLatency(us),"+timeout.getMaxTimeout()/1000);
		Core.println("");
	}
	public RequestTimeout getTimeoutByLoadType(LoadTypeEnum loadType) {
		RequestTimeout timeout=null;
		if(LoadTypeEnum.WRITE.getId().equals(loadType.getId())){
			timeout=writeTimeout;
		}
		if(LoadTypeEnum.RANDOM_INSERT.getId().equals(loadType.getId())){
			timeout=randomInsertTimeout;
		}
		if(LoadTypeEnum.UPDATE.getId().equals(loadType.getId())){
			timeout=updateTimeout;
		}
		if(LoadTypeEnum.SIMPLE_READ.getId().equals(loadType.getId())){
			timeout=simpleReadTimeout;
		}
		if(LoadTypeEnum.AGGRA_READ.getId().equals(loadType.getId())){
			timeout=analysisReadTimeout;
		}
		return timeout;
	}
	public double getCostTime() {
		return costTime;
	}
	public void setCostTime(double costTime) {
		this.costTime = costTime;
	}
}

