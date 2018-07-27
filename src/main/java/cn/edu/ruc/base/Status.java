package cn.edu.ruc.base;
/**
 * 请求数据库返回状态描述
 * @author sxg
 */
public class Status {
	/**
	 * 是否请求成功
	 * 1:成功
	 * 0:失败
	 */
	private int flag;
	/**
	 * 消耗时间
	 * 单位为毫秒
	 */
	private long costTime;
	/**
	 * 成功的记录数
	 */
	private int pointNum;
	/**
	 * 对数据库操作成功
	 * @param costTime 对数据库操作消耗的时间
	 * @return
	 */
	public static Status OK(long costTime){
		Status status=new Status();
		status.setCostTime(costTime);
		status.setFlag(1);
		return status;
	}
	/**
	 * 对数据库操作成功
	 * @param costTime 对数据库操作消耗的时间
	 * @param nums  对数据查询和写入的数据点数
	 * @return
	 */
	public static Status OK(long costTime,int nums){
		Status status=new Status();
		status.setCostTime(costTime);
		status.setFlag(1);
		status.setPointNum(nums);
		return status;
	}
	/**
	 * 对数据库操作失败
	 * @param costTime 对数据库操作消耗的时间
	 * @return
	 */
	public static Status FAILED(long costTime){
		Status status=new Status();
		status.setCostTime(costTime);
		status.setFlag(0);
		return status;
	}
	public boolean isOK(){
		if(1==flag){
			return true;
		}else{
			return false;
		}
	}
	public int getFlag() {
		return flag;
	}
	private void setFlag(int flag) {
		this.flag = flag;
	}
	public long getCostTime() {
		return costTime;
	}
	public void setCostTime(long costTime) {
		this.costTime = costTime;
	}
	private Status() {
		super();
	}
	public int getPointNum() {
		return pointNum;
	}
	public void setPointNum(int pointNum) {
		this.pointNum = pointNum;
	}
}

