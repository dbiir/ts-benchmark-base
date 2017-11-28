package cn.edu.ruc.biz.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import cn.edu.ruc.biz.Constants;
import cn.edu.ruc.enums.ReadTypeEnum;
/**
 * 
 */
public class ReadRecord implements Serializable{
	private static final long serialVersionUID = 1L;
	private Long id;
	private Long batchId=Constants.PERFORM_BATCH_ID;
	private Long readTime =System.currentTimeMillis();
	private Long targetTps;
	private Long realTps=0L;
	private Long avgTimeOut;
	private Long maxTimeOut;
	private Long minTimeOut;
	private Long th95TimeOut;
	private Integer readType;
	private Long read1Times =0L;
	private Long read2Times =0L;
	private Long read3Times=0L;
	private Long read4Times=0L;
	private Long read5Times=0L;
	private Long read6Times=0L;
	private Long read7Times=0L;
	private Long read8Times=0L;
	private Long read9Times=0L;
	private Long read10Times=0L;
	private Long read11Times=0L;
	private Long read12Times=0L;
	private Long simpleReadMaxTimes=0L;
	private Long simpleReadMinTimes=0L;
	private Long simpleReadAvgTimes=0L;
	private Long simpleReadCountTimes=0L;
	private List<Long> timeOutList=new CopyOnWriteArrayList<Long>();
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
	public Long getReadTime() {
		return readTime;
	}
	public void setReadTime(Long readTime) {
		this.readTime = readTime;
	}
	public Long getTargetTps() {
		return targetTps;
	}
	public void setTargetTps(Long targetTps) {
		this.targetTps = targetTps;
	}
	public Long getRealTps() {
		return realTps;
	}
	public synchronized void  addRealTps(){
		realTps++;
	}
	public Long getAvgTimeOut() {
		return avgTimeOut;
	}
	public Long getMaxTimeOut() {
		return maxTimeOut;
	}
	public Long getMinTimeOut() {
		return minTimeOut;
	}
	public Long getTh95TimeOut() {
		return th95TimeOut;
	}
	public Integer getReadType() {
		return readType;
	}
	public void setReadType(Integer readType) {
		this.readType = readType;
	}
	public Long getRead1Times() {
		return read1Times;
	}
	public synchronized void addRead1Times(){
		read1Times++;
	}
	public Long getRead2Times() {
		return read2Times;
	}
	public synchronized void addRead2Times() {
		this.read2Times++;
	}
	public Long getRead3Times() {
		return read3Times;
	}
	public synchronized void addRead3Times() {
		this.read3Times++;
	}
	public Long getRead4Times() {
		return read4Times;
	}
	public synchronized void addRead4Times() {
		this.read4Times++;
	}
	public Long getRead5Times() {
		return read5Times;
	}
	public synchronized void addRead5Times() {
		this.read5Times++;
	}
	public Long getRead6Times() {
		return read6Times;
	}
	public synchronized void addRead6Times() {
		this.read6Times++;
	}
	public Long getRead7Times() {
		return read7Times;
	}
	public synchronized void addRead7Times() {
		this.read7Times++;
	}
	public Long getRead8Times() {
		return read8Times;
	}
	public synchronized void addRead8Times() {
		this.read8Times++;
	}
	public Long getRead9Times() {
		return read9Times;
	}
	public synchronized void addRead9Times() {
		this.read9Times++;
	}
	public Long getRead10Times() {
		return read10Times;
	}
	public synchronized void addRead10Times() {
		this.read10Times++;
	}
	public Long getRead11Times() {
		return read11Times;
	}
	public synchronized void addRead11Times() {
		this.read11Times++;
	}
	public Long getRead12Times() {
		return read12Times;
	}
	public synchronized void addRead12Times() {
		this.read12Times++;
	}
	public List<Long> getTimeOutList() {
		return timeOutList;
	}
	public void addTimeOut(Long timeout){
		timeOutList.add(timeout);
	}
	public void computeTimeout(){
		if(timeOutList.size()!=0){
			Collections.sort(timeOutList);
			int size = timeOutList.size();
			maxTimeOut=timeOutList.get(size-1);
			minTimeOut=timeOutList.get(0);
			th95TimeOut=timeOutList.get((int)(0.95*size));//TODO 不太准确，需要优化
			long sumTimeOut=0;
			for(Long timeout:timeOutList){
				sumTimeOut+=timeout;
			}
			avgTimeOut=(long)(sumTimeOut/(double)size);
		}
	}
	@Override
	public String toString() {
		return "ReadRecord [batchId=" + batchId + ", readTime="
				+ readTime + ", targetTps=" + targetTps + ", realTps="
				+ realTps + ", avgTimeOut=" + avgTimeOut + ", maxTimeOut="
				+ maxTimeOut + ", minTimeOut=" + minTimeOut + ", th95TimeOut="
				+ th95TimeOut + ", readType=" + readType + ", read1Times="
				+ read1Times + ", read2Times=" + read2Times + ", read3Times="
				+ read3Times + ", read4Times=" + read4Times + ", read5Times="
				+ read5Times + ", read6Times=" + read6Times + ", read7Times="
				+ read7Times + ", read8Times=" + read8Times + ", read9Times="
				+ read9Times + ", read10Times=" + read10Times
				+ ", read11Times=" + read11Times + ", read12Times="
				+ read12Times + "]";
	}
	public void addReadTypeTimes(Integer readType) {
		if(readType==null){
			return;
		}
		if(ReadTypeEnum.SINGLE_READ_1.getId().equals(readType)){
			synchronized (this) {
				read1Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_2.getId().equals(readType)){
			synchronized (this) {
				read2Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_3.getId().equals(readType)){
			synchronized (this) {
				read3Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_4.getId().equals(readType)){
			synchronized (this) {
				read4Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_5.getId().equals(readType)){
			synchronized (this) {
				read5Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_6.getId().equals(readType)){
			synchronized (this) {
				read6Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_7.getId().equals(readType)){
			synchronized (this) {
				read7Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_8.getId().equals(readType)){
			synchronized (this) {
				read8Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_9.getId().equals(readType)){
			synchronized (this) {
				read9Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_10.getId().equals(readType)){
			synchronized (this) {
				read10Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_11.getId().equals(readType)){
			synchronized (this) {
				read11Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_READ_12.getId().equals(readType)){
			synchronized (this) {
				read12Times++;
			}
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_MAX_READ.getId().equals(readType)){
			synchronized (this) {
				simpleReadMaxTimes++;
			}
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_MIN_READ.getId().equals(readType)){
			synchronized (this) {
				simpleReadMinTimes++;
			}
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_AVG_READ.getId().equals(readType)){
			synchronized (this) {
				simpleReadAvgTimes++;
			}
		}
		if(ReadTypeEnum.SINGLE_SIMPLE_COUNT_READ.getId().equals(readType)){
			synchronized (this) {
				simpleReadCountTimes++;
			}
		}
	}
	public Long getSimpleReadMaxTimes() {
		return simpleReadMaxTimes;
	}
	public Long getSimpleReadMinTimes() {
		return simpleReadMinTimes;
	}
	public Long getSimpleReadAvgTimes() {
		return simpleReadAvgTimes;
	}
	public Long getSimpleReadCountTimes() {
		return simpleReadCountTimes;
	}
}

