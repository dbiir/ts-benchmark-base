package cn.edu.ruc.enums;
/**
 * 程序启动类型
 * @author sxg
 */
public enum StarupTypeEnum {
	LOAD_OFFLINE("load.offline","生成历史数据到硬盘，手动加载数据，数据生成器的功能")
	,LOAD_ONLINE("load.online","程序自动生成历史数据并导入"),
	PERFORM("perform","性能测试"), 
	SAP("sap","压力测试-写入"),//STRESS_APPEND 
	AS2S("as2s","给服务器施加压力");//ADD_STRESS_TO_SERVER
	private String value;
	private String desc;
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	private StarupTypeEnum(String value, String desc) {
		this.value = value;
		this.desc = desc;
	}
}

