package cn.edu.ruc.biz;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import cn.edu.ruc.TSUtils;
/**
 * 用于解析function.xml
 * @author sxg
 */
@XmlRootElement(name="functions")
public class FunctionXml {
	private List<FunctionParam> functions=new ArrayList<FunctionParam>();
	@XmlElement(name="function")
	public List<FunctionParam> getFunctions() {
		return functions;
	}

	public void setFunctions(List<FunctionParam> functions) {
		this.functions = functions;
	}
	public static void main(String[] args) {
		long timeByDateStr = TSUtils.getTimeByDateStr("2017-06-02 05:01:00");
		long millis = TimeUnit.HOURS.toMillis(4);
		Date date=new Date(timeByDateStr-timeByDateStr%millis);
		System.out.println(date);
	}
}

