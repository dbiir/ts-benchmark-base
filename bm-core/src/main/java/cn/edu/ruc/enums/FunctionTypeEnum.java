package cn.edu.ruc.enums;
/**
 * 函数类型
 * @author sxg
 */
public enum FunctionTypeEnum {
	LINE("float-mono","锯齿"),LINE_K("float-mono-k","线性函数"),SIN("float-sin","傅里叶函数"),SQUARE("float-square",""),RANDOM("float-random","随机数");
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
	private FunctionTypeEnum(String value, String desc) {
		this.value = value;
		this.desc = desc;
	}
}

