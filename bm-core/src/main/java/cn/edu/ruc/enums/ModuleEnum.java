package cn.edu.ruc.enums;
/**
 * 模块枚举
 * @author sxg
 */
public enum ModuleEnum {
	TIME_OUT("timeout","延迟时间"),
	CONCURRENT_THROUGHPUT("throughput_cc","并发测试-吞吐量"),//并发数/平均响应时间
	THROUGHPUT("throughput","正常测试-吞吐量"),//总请求数/总响应时间
	STRESS_APPEND("stress_append","写入-压力测试"),//写入-压力测试
	STRESS_UNAPPEND("stress_unappend","非写入-压力测试");//非写入-压力测试
	private String id;
	private String desc;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	private ModuleEnum(String id, String desc) {
		this.id = id;
		this.desc = desc;
	}
}

