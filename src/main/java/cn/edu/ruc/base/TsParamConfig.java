package cn.edu.ruc.base;

import cn.edu.ruc.cmd.CfgName;

/**
 * 
 * @author fasape
 */
public class TsParamConfig {
	private String batchCode;
	@CfgName(name="TEST_MODE")
	private String testMode;// read/write
	@CfgName(name="BACKGROUP_STATUS")
	private Integer backgroupStatus=0;//负载是否后台为背景负载，主要是为了让负载不是集中到某一点上
	@CfgName(name="DEVICE_NUM")
	private int deviceNum=20;
	@CfgName(name="SENSOR_NUM")
	private int sensorNum=26;
	@CfgName(name="CACHE_TIMES")
	private int cacheTimes=20;//每个设备缓存数据次数 
	@CfgName(name="WRITE_CLIENTS")
	private int writeClients=500;//写入客户端数  建议设备素可以整除客户端数
	@CfgName(name="START_TIME")
	private Long startTime=System.currentTimeMillis()-3600*1000*24;//数据开始时间 -1无穷 -2当前时间
	@CfgName(name="END_TIME")
	private Long endTime=System.currentTimeMillis();//数据结束时间 -1无穷 -2当前时间
	@CfgName(name="STEP")
	private Long step=7000L;//时间步长
	@CfgName(name="LOSE_RATIO")
	private double loseRatio=0.01;//丢失率
	@CfgName(name="WRITE_PULSE")
	private long writePulse;//写入时间间隔 单位为ms 
	
	@CfgName(name="READ_PERIOD")
	private Long readPeriod=30L;//读测试时长 单位为s
	@CfgName(name="READ_CLIENTS")
	private  int readClients=200;//查询的客户端数
	@CfgName(name="READ_SIMPLE_RATIO")
	private double readSimpleRatio=0.2;
	@CfgName(name="READ_AGGRE_RATIO")
	private double readAggreRatio=0.3;
	@CfgName(name="READ_SHRINK_RATIO")
	private double readShrinkRatio=0.5;
	@CfgName(name="READ_HIGN_RATIO")
	private double readHighRatio=0;
	public double getReadHighRatio() {
		return readHighRatio;
	}
	public void setReadHighRatio(double readHighRatio) {
		this.readHighRatio = readHighRatio;
	}
	@CfgName(name="READ_PULSE")
	private long readPulse;//读时间间隔 单位为ms 
	
	//函数比例
	@CfgName(name="FUNCTION_SIN_RATIO")
	private double sinRatio=0.036;
	@CfgName(name="FUNCTION_CONSTANT_RATIO")
	private double constantRatio=0.352;
	@CfgName(name="FUNCTION_RANDOM_RATIO")
	private double randomRatio=0.512;
	@CfgName(name="FUNCTION_SQUARE_RATIO")
	private double squareRatio=0.054;
	@CfgName(name="FUNCTION_LINE_RATIO")
	private double lineRatio=0.054;
	
	public int getDeviceNum() {
		return deviceNum;
	}
	public void setDeviceNum(int deviceNum) {
		this.deviceNum = deviceNum;
	}
	public int getSensorNum() {
		return sensorNum;
	}
	public void setSensorNum(int sensorNum) {
		this.sensorNum = sensorNum;
	}
	public int getCacheTimes() {
		return cacheTimes;
	}
	public void setCacheTimes(int cacheTimes) {
		this.cacheTimes = cacheTimes;
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
	public Long getStep() {
		return step;
	}
	public void setStep(Long step) {
		this.step = step;
	}
	public Long getReadPeriod() {
		return readPeriod;
	}
	public void setReadPeriod(Long readPeriod) {
		this.readPeriod = readPeriod;
	}
	public double getReadAggreRatio() {
		return readAggreRatio;
	}
	public void setReadAggreRatio(double readAggreRatio) {
		this.readAggreRatio = readAggreRatio;
	}
	public double getReadSimpleRatio() {
		return readSimpleRatio;
	}
	public void setReadSimpleRatio(double readSimpleRatio) {
		this.readSimpleRatio = readSimpleRatio;
	}
	public double getReadShrinkRatio() {
		return readShrinkRatio;
	}
	public void setReadShrinkRatio(double readShrinkRatio) {
		this.readShrinkRatio = readShrinkRatio;
	}
	public int getWriteClients() {
		return writeClients;
	}
	public void setWriteClients(int writeClients) {
		this.writeClients = writeClients;
	}
	public int getReadClients() {
		return readClients;
	}
	public void setReadClients(int readClients) {
		this.readClients = readClients;
	}
	public double getLoseRatio() {
		return loseRatio;
	}
	public void setLoseRatio(double loseRatio) {
		this.loseRatio = loseRatio;
	}
	public double getSinRatio() {
		return sinRatio;
	}
	public void setSinRatio(double sinRatio) {
		this.sinRatio = sinRatio;
	}
	public double getConstantRatio() {
		return constantRatio;
	}
	public void setConstantRatio(double constantRatio) {
		this.constantRatio = constantRatio;
	}
	public double getRandomRatio() {
		return randomRatio;
	}
	public void setRandomRatio(double randomRatio) {
		this.randomRatio = randomRatio;
	}
	public double getSquareRatio() {
		return squareRatio;
	}
	public void setSquareRatio(double squareRatio) {
		this.squareRatio = squareRatio;
	}
	public double getLineRatio() {
		return lineRatio;
	}
	public void setLineRatio(double lineRatio) {
		this.lineRatio = lineRatio;
	}
	public TsParamConfig() {
	}
	public String getTestMode() {
		return testMode;
	}
	public void setTestMode(String testMode) {
		this.testMode = testMode;
	}
	public void init() {
		//-1无穷 -2当前时间
		if(getStartTime().equals(-1L)) {
			setStartTime(0L);
		}
		if(getStartTime().equals(-2L)) {
			setStartTime(System.currentTimeMillis());
		}
		if(getEndTime().equals(-1L)) {
			setEndTime(Long.MAX_VALUE);
		}
		if(getEndTime().equals(-2L)) {
			setEndTime(System.currentTimeMillis());
		}
	}
	public long getWritePulse() {
		return writePulse;
	}
	public void setWritePulse(long writePulse) {
		this.writePulse = writePulse;
	}
	public long getReadPulse() {
		return readPulse;
	}
	public void setReadPulse(long readPulse) {
		this.readPulse = readPulse;
	}
	public String getBatchCode() {
		return batchCode;
	}
	public void setBatchCode(String batchCode) {
		this.batchCode = batchCode;
	}
	public Integer getBackgroupStatus() {
		return backgroupStatus;
	}
	public void setBackgroupStatus(Integer backgroupStatus) {
		this.backgroupStatus = backgroupStatus;
	}
}
