package cn.edu.ruc.cmd;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
/**
 * 标记该属性的对应cfg中的名称
 * @author fasape
 *
 */
@Target({TYPE, FIELD, METHOD})
@Retention(RUNTIME)
public @interface CfgName {
	/**
	 * cfg_*中的key
	 * @return
	 */
	String name() default "";
}
