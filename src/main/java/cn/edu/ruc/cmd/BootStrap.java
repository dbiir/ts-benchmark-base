package cn.edu.ruc.cmd;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
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
	private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

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
		//开始时间 结束时间 类型（读2/写1）sum costTime(s) 客户端数 pps/rps mean 50% 95% max min successRatio
		String resultFormat="%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.2f\n";
		String resultStr="";
		if("write".equals(tpc.getTestMode())) {
			LOGGER.info("...........write start...........");
			long startTime = System.currentTimeMillis();
			TsWriteResult result = biz.insertPoints();
			long endTime = System.currentTimeMillis();
			LOGGER.info("...........write end...........");
			jsonMap.put("result_write", result);
			resultStr=String.format(resultFormat,DATE_FORMAT.format(new Date(startTime)),
					DATE_FORMAT.format(new Date(endTime)),
					1,
					result.getSumPoints(),
					(endTime-startTime)/1000,
					tpc.getWriteClients(),
					result.getPps(),
					result.getMeanTimeout(),
					result.getFiftyTimeout(),
					result.getNinty5Timeout(),
					result.getMaxTimeout(),
					result.getMinTimeout(),
					1.0);
		}
		if("read".equals(tpc.getTestMode())) {
			LOGGER.info("...........read start...........");
			long startTime = System.currentTimeMillis();
			TsReadResult result = biz.queryTest();
			long endTime = System.currentTimeMillis();
			LOGGER.info("...........read end...........");
			jsonMap.put("result_read", result);
			resultStr=String.format(resultFormat,DATE_FORMAT.format(new Date(startTime)),
					DATE_FORMAT.format(new Date(endTime)),
					2,
					result.getSumRequests(),
					(endTime-startTime)/1000,
					tpc.getWriteClients(),
					result.getTps(),
					result.getMeanTimeout(),
					result.getFiftyTimeout(),
					result.getNinty5Timeout(),
					result.getMaxTimeout(),
					result.getMinTimeout(),
					result.getSuccessRatio());
		}
		String json = JSON.toJSONString(jsonMap);
		writeResult(resultStr);
//		writeResult(json);
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
//		String jsonPath = dbConfigPath.substring(0, index)+"/result/result.json";
		String jsonPath = dbConfigPath.substring(0, index)+"/result/result.csv";
		FileWriter fw = new FileWriter(jsonPath, true);
		PrintWriter out = new PrintWriter(fw);
//		out.write(json+",");
		out.write(json);
		out.println();
		fw.close();
		out.close();
	}
}
