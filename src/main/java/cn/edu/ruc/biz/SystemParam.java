package cn.edu.ruc.biz;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.Properties;

/**
 * 
 * @author fasape
 * 系统参数
 *   -| 运行参数 运行模式,目标测试库
 *   -| 数据生成参数
 *   -| 导入参数
 *   -| 写入参数
 *   -| 查询参数
 *  对应config.properties
 */
public class SystemParam {
//	# db info 目标测试数据库信息
//	# db_type 目标测试数据库
	public static String DB_TYPE;
	public static String DB_IP;
	public static int DB_PORT;
	public static String DB_USER="";
	public static String DB_PASSWD="";

//	# 测试模式 import ,insert_test ,overflow(random insert)
	public static String TEST_MODE;

//	# import mode 参数
//	#设备数
	public static int IMPORT_DEV_NUM=100;
//	#每个设备的传感器数
	public static int IMPORT_SENSOR_NUM=500;
//	#数据采集时间间隔 单位为ms
	public static int IMPORT_STEP=7000;
//	#数据每个客户端缓存条数 --一批发送多少条
	public static int IMPORT_CACHE_NUM=100000;
//	#客户端数/线程数
	public static int IMPORT_CLIENTS=50;
	public static double IMPORT_LOSE_RATIO=0.001;
	
	
	
//	# insert mode 参数
	public static int APPEND_MIN_DEV_NUM=1;
	public static int APPEND_MAX_DEV_NUM=10000;
	public static int APPEND_INTERVAL_DEV_NUM=10;
	public static int APPEND_SENSOR_NUM=500;
	public static int APPEND_STEP=7000;
//	# 写入加压测试每个客户端缓存条数
	public static int APPEND_CACHE_NUM=100000;
//	# 写入加压测试数据丢失率
	public static double APPEND_LOSE_RATIO=0.001;
//	# 写入客户端数
	public static int APPEND_CLIENTS=50;
//	# 测试环数
	public static int APPEND_LOOP=60;
	
	
//	# read mode 参数
//	#simple_read_test ,aggre_read_test ,shrink_read_test 比例 和必须是1
	public static double READ_SIMPLE_RATIO=1;
	public static double READ_AGGRE_RATIO=0;
	public static double READ_SHRINK_RATIO=0;

	public static String READ_AGGRE_TYPE;
	public static String READ_SHRINK_TYPE;

	public static int READ_MIN_CLINETS=1;
	public static int READ_MAX_CLINETS=100;
	public static int READ_TIMES=10;
	
//	# multi 参数 读写混合 读背景下写入，写背景下测读

//	# 混合模式测试读性能，写的设备数
	public static int MULTI_APPEND_DEVS=100;

//	# 混合模式测试写性能，读的客户端数
	public static int MULTI_READ_CLIENTS=30;



//	# 数据生成参数 各类函数比例，各类函数参数
//	#线性函数比例
	public static double FUNCTION_LINE_RATIO=0.054;
//	#傅里叶函数函数比例
	public static double FUNCTION_SIN_RATIO=0.036;
//	#方波函数比例
	public static double FUNCTION_SQUARE_RATIO= 0.054;
//	#随机数函数比例
	public static double FUNCTION_RANDOM_RATIO= 0.512;
//	#常数函数比例
	public static double FUNCTION_CONSTANT_RATIO= 0.352;
	
	private static boolean IS_INIT=false;
	public static String DB_CLASS;
	/**
	 * 是否已经初始话参数
	 * @return
	 */
	public static boolean isInit() {
		return IS_INIT;
	}
	public static void initParam() {
		String configPath = System.getProperty("config_path");
		Properties prop=new Properties();
		try {
			prop.load(new FileInputStream(new File(configPath)));
			SystemParam obj = SystemParam.class.newInstance();
			Field[] fields = SystemParam.class.getDeclaredFields();
			for(Field field:fields) {
				String fieldName=field.getName();
				if("IS_INIT".equals(fieldName)){
					continue;
				}
				if("DB_CLASS".equals(fieldName)){
					continue;
				}
				String value = prop.getProperty(fieldName, "");
				String fieldType = field.getType().getName();
				if(fieldType.indexOf("int")!=-1) {
					field.set(obj, Integer.parseInt(value));
				}else if(fieldType.indexOf("double")!=-1) {
					field.set(obj, Double.parseDouble(value));
				}else {
					field.set(obj, value);
				}
				System.out.println(fieldName+":"+value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		IS_INIT=true;
	}
	public static void main(String[] args) {
		initParam();
	}
}
