package cn.edu.ruc.enums;
/**
 * 阅读类型 
 */
public enum LoadTypeEnum {
	WRITE(1,"write"),
	RANDOM_INSERT(2,"random insert"),
	UPDATE(3,"update"),
	SIMPLE_READ(4,"simple read"),
	AGGRA_READ(5,"analysis read"), //查某段时间的聚合值
	SHRINK_READ(6,"shrink read"),//缩量查询 group by
	MUILTI(99,"混合");
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
	private LoadTypeEnum(Integer id, String desc) {
		this.id = id;
		this.desc = desc;
	}
}

