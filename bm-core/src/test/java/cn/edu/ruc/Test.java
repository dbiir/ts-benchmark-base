package cn.edu.ruc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
	public static void main(String[] args) {
		Logger log=LoggerFactory.getLogger(Test.class);
		log.info("nihao{},{}",'a','b');
		String sql="insert into book values(%s,%s,%s)";
		String format = String.format(sql, 1,2,"3");
		System.out.println(format);
	}
}
