package cn.edu.ruc.biz.model;

import cn.edu.ruc.biz.Constants;
import cn.edu.ruc.enums.LoadTypeEnum;

/**
 * 各个负载比例
 * @author sxg
 */
public class LoadRatio {
	private double writeEndRatio;
	private double randomInsertEndRatio;
	private double simpleQueryEndRatio;
	private double aggrQueryEndRatio;
	private double updateEndRatio;
	private double sumRatio;
	public LoadRatio(double writeRatio, double randomInsertRatio,
			double simpleQueryRatio, double aggrQueryRatio, double updateRatio) {
		super();
		if(writeRatio<0||randomInsertRatio<0||simpleQueryRatio<0||aggrQueryRatio<0||updateRatio<0){
			System.err.println("==");//TODO
			System.exit(0);
		}
		sumRatio=writeRatio+randomInsertRatio+simpleQueryRatio+aggrQueryRatio+updateRatio;
		if(sumRatio==0){
			System.err.println("=================");//TODO
			System.exit(0);
		}
		this.writeEndRatio=writeRatio/sumRatio;
		this.randomInsertEndRatio=writeEndRatio+(randomInsertRatio)/sumRatio;
		this.simpleQueryEndRatio=randomInsertEndRatio+(simpleQueryRatio)/sumRatio;
		this.aggrQueryEndRatio=simpleQueryEndRatio+(aggrQueryRatio)/sumRatio;
		this.updateEndRatio=aggrQueryEndRatio+(updateRatio)/sumRatio;
	}
	public double getWriteEndRatio() {
		return writeEndRatio;
	}
	public double getRandomInsertEndRatio() {
		return randomInsertEndRatio;
	}
	public double getSimpleQueryEndRatio() {
		return simpleQueryEndRatio;
	}
	public double getAggrQueryEndRatio() {
		return aggrQueryEndRatio;
	}
	public double getUpdateEndRatio() {
		return updateEndRatio;
	}
	public double getWriteStartRatio() {
		return 0;
	}
	public double getRandomInsertStartRatio() {
		return writeEndRatio;
	}
	public double getSimpleQueryStartRatio() {
		return randomInsertEndRatio;
	}
	public double getAggrQueryStartRatio() {
		return simpleQueryEndRatio;
	}
	public double getUpdateStartRatio() {
		return aggrQueryEndRatio;
	}
	public static LoadRatio newInstanceByLoadType(Integer loadType){
		if(LoadTypeEnum.WRITE.getId().equals(loadType)){
			return new LoadRatio(1, 0, 0, 0, 0);
		}
		if(LoadTypeEnum.RANDOM_INSERT.getId().equals(loadType)){
			return new LoadRatio(0, 1, 0, 0, 0);
		}
		if(LoadTypeEnum.SIMPLE_READ.getId().equals(loadType)){
			return new LoadRatio(0,0,1, 0, 0);
		}
		if(LoadTypeEnum.AGGRA_READ.getId().equals(loadType)){
			return new LoadRatio(0, 0, 0, 1, 0);
		}
		if(LoadTypeEnum.UPDATE.getId().equals(loadType)){
			return new LoadRatio(0, 0, 0, 0, 1);
		}
		if(LoadTypeEnum.MUILTI.getId().equals(loadType)){
			return new LoadRatio(Constants.WRITE_RATIO,Constants.RANDOM_INSERT_RATIO,Constants.SIMPLE_QUERY_RATIO, Constants.MAX_QUERY_RATIO,Constants.UPDATE_RATIO);
		}
		System.err.println("LoadRatio.newInstanceByLoadType(Integer loadType),loadType异常[loadType="+loadType+"]");
		System.exit(0);
		return null;
	}
}

