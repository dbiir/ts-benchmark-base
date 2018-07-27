package cn.edu.ruc.adapter;

import cn.edu.ruc.base.Status;
import cn.edu.ruc.base.TsDataSource;
import cn.edu.ruc.base.TsParamConfig;
import cn.edu.ruc.base.TsQuery;
import cn.edu.ruc.base.TsWrite;

/**
 * 根适配器
 * 主要用于适配目标测试数据库
 * @author fasape
 *
 */
public interface DBAdapter {
	/**
	 * 初始化数据源和存储结构
	 * @param dataSource 数据源连接参数
	 */
	public void initDataSource(TsDataSource ds,TsParamConfig tspc);
	/**
	 * 写入预处理
	 */
	public Object preWrite(TsWrite tsWrite);
	/**
	 * 执行写入预处理
	 */
	public Status execWrite(Object write);
	/**
	 * 查询预处理
	 */
	public Object preQuery(TsQuery tsQuery);
	/**
	 * 执行查询预处理
	 */
	public Status execQuery(Object query);
	/**
	 * 校验数据操作结果
	 */
	
	/**
	 * 关闭当前适配器对象
	 */
	public void closeAdapter();
}
