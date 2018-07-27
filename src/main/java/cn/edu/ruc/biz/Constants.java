package cn.edu.ruc.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.edu.ruc.utils.TSUtils;

/**
 * 系统运行常量值
 * @author sxg
 */
public class Constants {
	/**LoadBatchId 批次id*/
	public static  Long LOAD_BATCH_ID;
	/**
	 * 程序启动类型list
	 */
	public static List<String> MODULES=new ArrayList<String>();
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
	public static void initLoadTypeRatio() {
	}
}

