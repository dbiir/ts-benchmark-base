package cn.edu.ruc.biz;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import cn.edu.ruc.TSUtils;
import cn.edu.ruc.db.DBBase;
import cn.edu.ruc.enums.ModuleEnum;

/**
 * 系统运行常量值
 * @author sxg
 */
public class Constants {
	public static Properties PROPERTIES;
	/**LoadBatchId 批次id*/
	public static  Long LOAD_BATCH_ID;
	/**目标测试数据库类*/
	public static String DB_CLASS="";
	/**程序启动类型*/
	public static String STARUP_TYPE="";
	/**
	 * 程序启动类型list
	 */
	public static List<String> MODULES=new ArrayList<String>();
	/**历史数据状态*/
	public static Integer DATA_STATUS=0;
	/**设备数量*/
	public static int DEVICE_NUMBER=100;
	/**每个设备的传感器数量*/
	public static int SENSOR_NUMBER=500;
	/**数据采集步长*/
	public static long POINT_STEP=7000;
	/**数据发送缓存条数*/
	public static int CACHE_POINT_NUM=100000;
	public static int CLIENTS=100;
	/**数据采集丢失率*/
	public static double POINT_LOSE_RATIO=0.01;
	// ============各函数比例start============//FIXME 传参数时加上这几个参数
	/**线性  默认  9个  0.054*/
	public static double LINE_RATIO=0.054;
	/**傅里叶函数  6个  0.036*/
//	public static double SIN_RATIO=0.386;//0.036
	public static double SIN_RATIO=0.036;//0.036
	/**方波 9个 0.054*/
	public static double SQUARE_RATIO= 0.054;
	/**随机数 默认  86个  0.512*/
	public static double RANDOM_RATIO= 0.512;
	/**常数  默认  58个  0.352  */
//	public static double CONSTANT_RATIO= 0.002;//0.352
	public static double CONSTANT_RATIO= 0.352;//0.352
	
	// ============各函数比例end============
	
	/**内置函数参数*/
	public static final List<FunctionParam> LINE_LIST=new ArrayList<FunctionParam>();
	public static final List<FunctionParam> SIN_LIST=new ArrayList<FunctionParam>();
	public static final List<FunctionParam> SQUARE_LIST=new ArrayList<FunctionParam>();
	public static final List<FunctionParam> RANDOM_LIST=new ArrayList<FunctionParam>();
	public static final List<FunctionParam> CONSTANT_LIST=new ArrayList<FunctionParam>();
	/**设备编号*/
	public static final List<String> DEVICE_CODES=new ArrayList<String>();
	/**传感器编号*/
	public static final List<String> SENSOR_CODES=new ArrayList<String>();
	/**设备_传感器 时间偏移量*/
	public static final Map<String,Long> SHIFT_TIME_MAP=new HashMap<String,Long>();
	/**传感器对应的函数*/
	public static final Map<String,FunctionParam> SENSOR_FUNCTION=new HashMap<String, FunctionParam>();
	
	/**历史数据开始时间*/
	public static Long HISTORY_START_TIME;
	/**历史数据结束时间*/
	public static Long HISTORY_END_TIME;
	
	/**当前测试的数据库实例*/
	private static DBBase DB_BASE;
	
	//负载生成器参数 start
	/**LoadBatchId 批次id*/
	public static  Long PERFORM_BATCH_ID;
	//负载测试完是否删除数据
	public static boolean IS_DELETE_DATA=false;
	public static Double WRITE_RATIO=0.2;
	public static Double SIMPLE_QUERY_RATIO=0.2;
	public static Double MAX_QUERY_RATIO=0.2;
	public static Double MIN_QUERY_RATIO=0.2;
	public static Double AVG_QUERY_RATIO=0.2;
	public static Double COUNT_QUERY_RATIO=0.2;
	public static Double SUM_QUERY_RATIO=0.2;
	public static Double RANDOM_INSERT_RATIO=0.2;
	public static Double UPDATE_RATIO=0.2;
	/**写入而是的设备号前缀*/
	public static String INSERT_PERFRM_DEVICE_PREFIX="dp_"+TSUtils.getRandomLetter(1);
	//负载生成器参数 end
	
	
	
	public static List<String> getAllSensors(){
		return SENSOR_CODES;
	}
	public static List<String> getAllDevices(){
		return DEVICE_CODES;
	}
	
	public static void printShiftTimeMap(){
		Set<String> keySet = SHIFT_TIME_MAP.keySet();
		System.out.println("=============打印 SHIFT_TIME_MAP=============");
		for(String key:keySet){
			System.out.println("key:"+key+"|value:"+SHIFT_TIME_MAP.get(key));
		}
	}
	public static void printSernFunction(){
		Set<String> keySet = SENSOR_FUNCTION.keySet();
		System.out.println("=============打印 SHIFT_TIME_MAP=============");
		for(String key:keySet){
			System.out.println("key:"+key+"|value:"+SENSOR_FUNCTION.get(key));
		}
	}
	/**
	 * 获取测试数据库
	 * FIXME 可优化  添加properties
	 * @param props
	 * @return
	 */
	public static DBBase getDBBase(){
		try {
			if(DB_BASE==null){
				Class<?> dbClass  = Class.forName(PROPERTIES.getProperty(Core.DB_CLASS_PROERTITY));
				DB_BASE = (DBBase)dbClass.newInstance();
				DB_BASE.setProperties(PROPERTIES);;
				DB_BASE.init();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return DB_BASE;
	}
	public static FunctionParam getFunctionByFunctionTypeAndId(String functionType,String functionId){
		if(functionType.indexOf("-mono-k")!=-1){
			for(FunctionParam param:LINE_LIST){
				if(functionId.equals(param.getId())){
					return param;
				}
			}
		}else if(functionType.indexOf("-mono")!=-1){
			for(FunctionParam param:CONSTANT_LIST){
				if(functionId.equals(param.getId())){
					return param;
				}
			}
		}else if(functionType.indexOf("-sin")!=-1){
			for(FunctionParam param:SIN_LIST){
				if(functionId.equals(param.getId())){
					return param;
				}
			}
		}else if(functionType.indexOf("-square")!=-1){
			for(FunctionParam param:SQUARE_LIST){
				if(functionId.equals(param.getId())){
					return param;
				}
			}
		}else if(functionType.indexOf("-random")!=-1){
			for(FunctionParam param:RANDOM_LIST){
				if(functionId.equals(param.getId())){
					return param;
				}
			}
		}
		return null;
	}
	/**
	 * 初始化性能测试功能
	 */
	public static void initModules(){
		String moduleStr = PROPERTIES.getProperty(Core.MODULES_PROPERTY,"");
		if(StringUtils.isBlank(moduleStr)){
//			ModuleEnum[] values = ModuleEnum.values();
//			for(ModuleEnum module:values){
//				MODULES.add(module.getId());
//			}
			Core.println("when loadType is perform ,param -modules is necessary");
			System.exit(0);
		}else{
			String[] modules = moduleStr.split(",");
			MODULES.addAll(Arrays.asList(modules));
		}
	}
	public static void initLoadTypeRatio() {
		WRITE_RATIO=Double.parseDouble(PROPERTIES.getProperty(Core.TP_RATIO_WIRTE_PROP, "0"));
		RANDOM_INSERT_RATIO=Double.parseDouble(PROPERTIES.getProperty(Core.TP_RATIO_RANDOMWRITE_PROP,  "0"));
		MAX_QUERY_RATIO=Double.parseDouble(PROPERTIES.getProperty(Core.TP_RATIO_ANALOSIS_READ_PROP,  "0"));
		SIMPLE_QUERY_RATIO=Double.parseDouble(PROPERTIES.getProperty(Core.TP_RATIO_SIMPLE_READ_PROP,  "0"));
		UPDATE_RATIO=Double.parseDouble(PROPERTIES.getProperty(Core.TP_RATIO_UPDATE_PROP,  "0"));
		if(WRITE_RATIO<0||RANDOM_INSERT_RATIO<0||MAX_QUERY_RATIO<0||SIMPLE_QUERY_RATIO<0||UPDATE_RATIO<0){
			Core.printlnErr("负载比例不得<0");
			System.exit(0);
		}
		if(WRITE_RATIO==0D&&RANDOM_INSERT_RATIO==0D&&MAX_QUERY_RATIO==0D&&SIMPLE_QUERY_RATIO==0D&&UPDATE_RATIO==0D){
			WRITE_RATIO=0.2;
			SIMPLE_QUERY_RATIO=0.2;
			MAX_QUERY_RATIO=0.2;
			MIN_QUERY_RATIO=0.2;
			AVG_QUERY_RATIO=0.2;
			COUNT_QUERY_RATIO=0.2;
			SUM_QUERY_RATIO=0.2;
			RANDOM_INSERT_RATIO=0.2;
			UPDATE_RATIO=0.2;
		}
	}
}

