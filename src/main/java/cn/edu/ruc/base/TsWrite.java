package cn.edu.ruc.base;

import java.io.Serializable;
import java.util.LinkedList;
/**
 * 
 * @author fasape
 *
 */
public class TsWrite implements Serializable{
	private static final long serialVersionUID = 1L;
	//n个数据包
	private LinkedList<TsPackage> pkgs=new LinkedList<TsPackage>();
	public void append(TsPackage pkg) {
		pkgs.add(pkg);
	}
	public LinkedList<TsPackage> getPkgs(){
		return pkgs;
	}
	public int getPointsNum() {
		int size=0;
		for(TsPackage pkg:pkgs) {
			size+=pkg.getSensorCodes().size();
		}
		return size;
	}
}
