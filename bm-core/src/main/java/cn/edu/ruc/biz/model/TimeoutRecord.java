package cn.edu.ruc.biz.model;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import cn.edu.ruc.biz.Constants;
import cn.edu.ruc.enums.LoadTypeEnum;

/**
 * 性能测试(指标为延迟时间) 记录
 * 输出单位 
 */
public class TimeoutRecord {
	private long id;
	private long batchId=Constants.PERFORM_BATCH_ID;
	private long time=System.currentTimeMillis();
	private int loadType;
	private long targetTimes;
	private int threads;
	//各个的成功条数
	private int writeTimes;
	private int randomInsertTimes;
	private int simpleReadTimes;
	private int aggreReadTimes;
	private int updateTimes;
	
	//性能指标 单位为us
	private long timeoutMax;
	private long timeoutMin;
	private long timeoutAvg;
	private long timeoutTh50;
	private long timeoutTh95;
	private long timeoutSum;//总延迟
	//成功次数  10s 内响应成功 //FIXME
	private long successTimes;
	//失败次数  10s 未响应成功//FIXME
	private long failedTimes;
	
	
	//
	private List<Long> timeoutList=new CopyOnWriteArrayList<Long>();
	public long getId() {
		return id;
	}
	public long getBatchId() {
		return batchId;
	}
	public long getTime() {
		return time;
	}
	public int getLoadType() {
		return loadType;
	}
	public int getWriteTimes() {
		return writeTimes;
	}
	public int getRandomInsertTimes() {
		return randomInsertTimes;
	}
	public int getSimpleReadTimes() {
		return simpleReadTimes;
	}
	public int getAggreReadTimes() {
		return aggreReadTimes;
	}
	public int getUpdateTimes() {
		return updateTimes;
	}
	public long getTimeoutMax() {
		return timeoutMax;
	}
	public long getTimeoutMin() {
		return timeoutMin;
	}
	public long getTimeoutAvg() {
		return timeoutAvg;
	}
	public long getTimeoutTh50() {
		return timeoutTh50;
	}
	public long getTimeoutTh95() {
		return timeoutTh95;
	}
	public long getTimeoutSum() {
		return timeoutSum;
	}
	public long getSuccessTimes() {
		return successTimes;
	}
	public long getFailedTimes() {
		return failedTimes;
	}
	/**
	 * @param loadType
	 * @param timeout
	 */
	public  void  addSuccessTimes(Integer loadType,long timeout){
		if(LoadTypeEnum.WRITE.getId().equals(loadType)){
			synchronized (this) {
				timeoutList.add(timeout);
				writeTimes++;
				successTimes++;
			}
		}
		if(LoadTypeEnum.RANDOM_INSERT.getId().equals(loadType)){
			synchronized (this) {
				timeoutList.add(timeout);
				randomInsertTimes++;
				successTimes++;
			}
		}
		if(LoadTypeEnum.UPDATE.getId().equals(loadType)){
			synchronized (this) {
				timeoutList.add(timeout);
				updateTimes++;
				successTimes++;
			}
		}
		if(LoadTypeEnum.SIMPLE_READ.getId().equals(loadType)){
			synchronized (this) {
				timeoutList.add(timeout);
				simpleReadTimes++;
				successTimes++;
			}
		}
		if(LoadTypeEnum.AGGRA_READ.getId().equals(loadType)){
			synchronized (this) {
				timeoutList.add(timeout);
				aggreReadTimes++;
				successTimes++;
			}
		}
	}
	public synchronized void  addFailedTimes(){
		failedTimes++;
	}
	public void computeTimeout(){
		if(timeoutList.size()!=0){
			Collections.sort(timeoutList);
			int size = timeoutList.size();
			timeoutMax=TimeUnit.NANOSECONDS.toMicros(timeoutList.get(size-1));
			timeoutMin=TimeUnit.NANOSECONDS.toMicros(timeoutList.get(0));
			timeoutTh95=TimeUnit.NANOSECONDS.toMicros(timeoutList.get((int)(0.95*size)));//TODO 不太准确，需要优化
			timeoutTh50=TimeUnit.NANOSECONDS.toMicros(timeoutList.get((int)(0.50*size)));//TODO 不太准确，需要优化
			for(Long timeout:timeoutList){
				timeoutSum+=TimeUnit.NANOSECONDS.toMicros(timeout);
			}
			timeoutAvg=(long)(timeoutSum/(double)size);
		}
	}
	public void setLoadType(int loadType) {
		this.loadType = loadType;
	}
	/**
	 * 
	 * @param loadType 负载类型
	 * @param targetTimes 目标总请求次数
	 */
	public TimeoutRecord(int loadType,long targetTimes,int threads) {
		super();
		this.loadType = loadType;
		this.targetTimes = targetTimes;
		this.threads = threads;
	}
	public long getTargetTimes() {
		return targetTimes;
	}
	public int getThreads() {
		return threads;
	}
	@Override
	public String toString() {
		return "TimeoutRecord [batchId=" + batchId + ", time=" +new Date(time)
				+ ", loadType=" + loadType + ", targetTimes=" + targetTimes
				+ ", threads=" + threads + ", writeTimes=" + writeTimes
				+ ", randomInsertTimes=" + randomInsertTimes
				+ ", simpleReadTimes=" + simpleReadTimes + ", aggreReadTimes="
				+ aggreReadTimes + ", updateTimes=" + updateTimes
				+ ", timeoutMax=" + timeoutMax + ", timeoutMin=" + timeoutMin
				+ ", timeoutAvg=" + timeoutAvg + ", timeoutTh50=" + timeoutTh50
				+ ", timeoutTh95=" + timeoutTh95 + ", timeoutSum=" + timeoutSum
				+ ", successTimes=" + successTimes + ", failedTimes="
				+ failedTimes + "]";
	}
}

