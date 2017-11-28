package cn.edu.ruc;

import java.io.Serializable;
/**
 * 
 */
public class TimeSlot implements Serializable {
	private static final long serialVersionUID = 1L;
	private Long startTime;
	private Long endTime;
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
	public TimeSlot(Long startTime, Long endTime) {
		super();
		this.startTime = startTime;
		this.endTime = endTime;
	}
	public TimeSlot() {
		super();
	}
}

