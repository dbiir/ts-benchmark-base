package cn.edu.ruc.cmd;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import cn.edu.ruc.base.TsDataSource;
import cn.edu.ruc.base.TsParamConfig;
import cn.edu.ruc.base.TsReadResult;
import cn.edu.ruc.base.TsWriteResult;
import cn.edu.ruc.biz.CoreBiz;

/**
 * 引导类
 * @author fasape
 *
 */
public class BootStrap {
//	private static final String SYS_CONFIG_PATH="config_path";
	private static final String DB_CONFIG_PATH="db_config_path";
//	private static final String SYS_BINDING_PATH="bindings_path";
	private static Properties SYSTEM_PARAM=new Properties();
	private static Properties DB_PARAM=new Properties();
	private static Properties BINDING_PARAM=new Properties();
	private static final Logger LOGGER=LoggerFactory.getLogger(BootStrap.class);
	public static void main(String[] args) throws Exception {
		initParam(args);
		TsParamConfig tpc=getTsParamConfig();
		TsDataSource tsds=getTsDataSource();
		Map<String,Object> jsonMap=new HashMap<String,Object>();
		jsonMap.put("param_config", tpc);
		jsonMap.put("data_source", tsds);
		System.out.println(tsds.getDriverClass());
		CoreBiz biz=new CoreBiz(tpc, tsds);
		if("write".equals(tpc.getTestMode())) {
			LOGGER.info("...........write start...........");
			TsWriteResult result = biz.insertPoints();
			LOGGER.info("...........write end...........");
			jsonMap.put("result_write", result);

		}
		if("read".equals(tpc.getTestMode())) {
			LOGGER.info("...........read start...........");
			TsReadResult result = biz.queryTest();
			LOGGER.info("...........read end...........");
			jsonMap.put("result_read", result);
		}
		String json = JSON.toJSONString(jsonMap);
		writeResult(json);
	}
	private static void initParam(String[] args) {
		Options options=new Options();
		Option config = Option.builder("cf").argName("cfg name").hasArg().desc("Config file path (required)").build();
		Option bd = Option.builder("bd").argName("bd name").hasArg().desc("Binding file path (optional)").build();
		Option db = Option.builder("db").argName("db name").hasArg().desc("db config file path (required)").build();
		options.addOption(config);
		options.addOption(bd);
		options.addOption(db);
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine commandLine = parser.parse(options, args);
			String configPath=commandLine.getOptionValue("cf");
			String bingdingsPath=commandLine.getOptionValue("bd");
			String dbConfigPath=commandLine.getOptionValue("db");
//			System.setProperty(SYS_BINDING_PATH, bingdingsPath);
//			System.setProperty(SYS_CONFIG_PATH, configPath);
			System.setProperty(DB_CONFIG_PATH, dbConfigPath);
			SYSTEM_PARAM.load(new FileInputStream(new File(configPath)));
			DB_PARAM.load(new FileInputStream(new File(dbConfigPath)));
			BINDING_PARAM.load(new FileInputStream(new File(bingdingsPath)));
			Properties prop=new Properties();
			prop.load(new FileInputStream(new File(bingdingsPath)));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static TsParamConfig getTsParamConfig() {
		//反射+注解获取
		TsParamConfig tpc=new TsParamConfig();
		Field[] fields = tpc.getClass().getDeclaredFields();
		for(Field f:fields) {
			String fieldName = f.getName();
			CfgName cfg = f.getDeclaredAnnotation(CfgName.class);
			if(cfg==null) {
				continue;
			}
			String cfgName = cfg.name();
			try {
				String cfgValue = SYSTEM_PARAM.getProperty(cfgName);
				if(StringUtils.isNotBlank(cfgValue)) {
					BeanUtils.setProperty(tpc, fieldName, cfgValue);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		tpc.init();
		return tpc;
	}
	private static TsDataSource getTsDataSource() {
		TsDataSource tsds=new TsDataSource();
		Field[] fields = tsds.getClass().getDeclaredFields();
		for(Field f:fields) {
			String fieldName = f.getName();
			CfgName cfg = f.getDeclaredAnnotation(CfgName.class);
			if(cfg==null) {
				continue;
			}
			String cfgName = cfg.name();
			try {
				String cfgValue = DB_PARAM.getProperty(cfgName);
				BeanUtils.setProperty(tsds, fieldName, cfgValue);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		tsds.setDriverClass(BINDING_PARAM.getProperty(tsds.getDbType()));
		return tsds;
	}
	private static void writeResult(String json) throws Exception {
		String dbConfigPath = System.getProperty(DB_CONFIG_PATH);
		int index=dbConfigPath.lastIndexOf("/conf");
		String jsonPath = dbConfigPath.substring(0, index)+"/result/result.json";
		FileWriter fw = new FileWriter(jsonPath, true);
		PrintWriter out = new PrintWriter(fw);
		out.write(json+",");
		out.println();
		fw.close();
		out.close();
	}
}
