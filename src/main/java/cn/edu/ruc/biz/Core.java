package cn.edu.ruc.biz;

import java.io.InputStream;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

/**
 * 
 * @author sxg
 */
public class Core {
	/**.
	 * 初始化内置函数
	 * functionParam
	 */
	public static void initInnerFucntion() {
		if(Constants.LINE_LIST.size()>0||Constants.CONSTANT_LIST.size()>0||Constants.SIN_LIST.size()>0||Constants.SQUARE_LIST.size()>0||Constants.RANDOM_LIST.size()>0){
			return;//已初始化，不需要再次初始化内置函数
		}
		FunctionXml xml=null;
		try {
			InputStream input = Core.class.getResourceAsStream("function.xml");
			JAXBContext context = JAXBContext.newInstance(FunctionXml.class,FunctionParam.class);
			Unmarshaller unmarshaller = context.createUnmarshaller(); 
			xml = (FunctionXml)unmarshaller.unmarshal(input);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		List<FunctionParam> xmlFuctions = xml.getFunctions();
		for(FunctionParam param:xmlFuctions){
			if(param.getFunctionType().indexOf("-mono-k")!=-1){
				Constants.LINE_LIST.add(param);
			}else if(param.getFunctionType().indexOf("-mono")!=-1){
				//如果min==max则为常数，系统没有非常数的
				if(param.getMin()==param.getMax()){
					Constants.CONSTANT_LIST.add(param);
				}
			}else if(param.getFunctionType().indexOf("-sin")!=-1){
				Constants.SIN_LIST.add(param);
			}else if(param.getFunctionType().indexOf("-square")!=-1){
				Constants.SQUARE_LIST.add(param);
			}else if(param.getFunctionType().indexOf("-random")!=-1){
				Constants.RANDOM_LIST.add(param);
			}
		}
	}
}

