package cn.edu.ruc.enums;
/**
 * 阅读类型 
 */
public enum ReadTypeEnum {
	SINGLE_READ_1(1,"单读"),
	SINGLE_READ_2(2,"单读"),
	SINGLE_READ_3(3,"单读"),
	SINGLE_READ_4(4,"单读"),
	SINGLE_READ_5(5,"单读"),
	SINGLE_READ_6(6,"单读"),
	SINGLE_READ_7(7,"单读"),
	SINGLE_READ_8(8,"单读"),
	SINGLE_READ_9(9,"单读"),
	SINGLE_READ_10(10,"单读"),
	SINGLE_READ_11(11,"单读"),
	SINGLE_READ_12(12,"单读"),
	SINGLE_SIMPLE_MAX_READ(13,"简单读一段时间内的最大值"),
	SINGLE_SIMPLE_MIN_READ(14,"简单读一段时间内的最小值"),
	SINGLE_SIMPLE_AVG_READ(15,"简单读一段时间内的平均值"),
	SINGLE_SIMPLE_COUNT_READ(16,"简单读一段时间内的总数"),
	MUILTI_READ(99,"复合读");
	private Integer id;
	private String desc;
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	private ReadTypeEnum(Integer id, String desc) {
		this.id = id;
		this.desc = desc;
	}
}

