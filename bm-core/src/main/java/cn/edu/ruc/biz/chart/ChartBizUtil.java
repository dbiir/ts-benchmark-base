package cn.edu.ruc.biz.chart;

import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jfree.chart.ChartColor;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.ItemLabelAnchor;
import org.jfree.chart.labels.ItemLabelPosition;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer3D;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.TextAnchor;

import cn.edu.ruc.biz.db.BizDBUtils;
import cn.edu.ruc.enums.LoadTypeEnum;

/**
 * 生成图表类
 * @author sxg
 */
public class ChartBizUtil {
	static{
	    StandardChartTheme mChartTheme = new StandardChartTheme("CN");
	    mChartTheme.setLargeFont(new Font("黑体", Font.BOLD, 20));
	    mChartTheme.setExtraLargeFont(new Font("宋体", Font.PLAIN, 15));
	    mChartTheme.setRegularFont(new Font("宋体", Font.PLAIN, 15));
	    ChartFactory.setChartTheme(mChartTheme);
	}
	/**
	 * 生成导入图表
	 * @param dbName 目标数据库名称
	 * @param loadBatchId 导入批次
	 * @param path 附件路径
	 */
	public static void generateLoadDataChart(String dbName,long loadBatchId){
		String path=getChartRootPath(dbName+"_LoadDataPerform");
		System.out.println(path);
	    XYSeries values = new XYSeries(dbName);
	    List<Map<String, Object>> list = BizDBUtils.selectListBySqlAndParam("select pps from ts_load_record where load_batch_id=?",loadBatchId);
	    System.out.println(list);
	    if(list!=null&&list.size()>0){
	    	for(int i=0;i<list.size();i++){
	    		Map<String, Object> map = list.get(i);
	    		values.add(i+1,((Number)map.get("pps")).doubleValue());
	    	}
	    }
	    XYSeriesCollection mCollection = new XYSeriesCollection();
	    mCollection.addSeries(values);
	    JFreeChart mChart= createXYLineChart("数据导入吞吐量折线图", "次数","speed(points/sec)",mCollection);
	    saveAsFile(mChart,path, 12000, 800);
	}
	public static void generateThroughputPerformChart(String dbName,long performBatchId){
		String path=getChartRootPath(dbName+"_ThroughputPerform");
		System.out.println(path);
	    XYSeries values = new XYSeries(dbName);
	    List<Map<String, Object>> list = BizDBUtils.selectListBySqlAndParam("select success_times,timeout_avg  from ts_timeout_perform where  perform_batch_id=? and load_type=99",performBatchId);
	    if(list!=null&&list.size()>0){
	    	for(int i=0;i<list.size();i++){
	    		Map<String, Object> map = list.get(i);
	    		double timeout=((Number)map.get("timeout_avg")).doubleValue();
	    		double successTimes=((Number)map.get("success_times")).doubleValue();
	    		if(successTimes==0){
	    			values.add(i+1,0);
	    		}else{
	    			values.add(i+1,successTimes*(TimeUnit.SECONDS.toMicros(1)/timeout));
	    		}
	    	}
	    }
	    XYSeriesCollection mCollection = new XYSeriesCollection();
	    mCollection.addSeries(values);
	    JFreeChart mChart= createXYLineChart("数据吞吐量折线图", "请求次数","吞吐量(requests/sec)",mCollection);
	    //saveAsFile(mChart,path, 1200, 800);
	    saveAsFile(mChart,path, 2400, 800);
	    
	}
	public static void generateInsertTimeoutPerformChart(String dbName,long performBatchId){
		String pngName=dbName+"insert_timeout";
	    generateTimeoutPerformChart(dbName, performBatchId,getChartRootPath(pngName),LoadTypeEnum.WRITE);
	}
	public static void generateRandomInsertTimeoutPerformChart(String dbName,long performBatchId){
		String pngName=dbName+"random_insert_timeout";
		generateTimeoutPerformChart(dbName, performBatchId, getChartRootPath(pngName),LoadTypeEnum.RANDOM_INSERT);
	}
	public static void generateUpdateTimeoutPerformChart(String dbName,long performBatchId){
		String pngName=dbName+"update_timeout";
		generateTimeoutPerformChart(dbName, performBatchId, getChartRootPath(pngName),LoadTypeEnum.UPDATE);
	}
	public static void generateSimpleReadTimeoutPerformChart(String dbName,long performBatchId){
		String pngName=dbName+"simple_read_timeout";
		generateTimeoutPerformChart(dbName, performBatchId, getChartRootPath(pngName),LoadTypeEnum.SIMPLE_READ);
	}
	public static void generateAggreReadTimeoutPerformChart(String dbName,long performBatchId){
		String pngName=dbName+"aggregate_read_timeout";
		generateTimeoutPerformChart(dbName, performBatchId, getChartRootPath(pngName),LoadTypeEnum.AGGRA_READ);
	}
	
	public static void generateTimeoutPerformChart(String dbName,long performBatchId,String path,LoadTypeEnum loadTypeEnum){
		DefaultCategoryDataset values = new DefaultCategoryDataset();   
	    List<Map<String, Object>> list = BizDBUtils.selectListBySqlAndParam("select target_times,timeout_max,timeout_min,timeout_avg,timeout_th50,timeout_th95 from ts_timeout_perform where  perform_batch_id=? and load_type=?",performBatchId,loadTypeEnum.getId());
	    System.out.println(list);
	    if(list!=null&&list.size()>0){
	    	for(int i=0;i<list.size();i++){
	    		Map<String, Object> map = list.get(i);
	    		values.addValue(((Number)map.get("timeout_max")).doubleValue(),dbName,"timeout_max");
	    		values.addValue(((Number)map.get("timeout_min")).doubleValue(),dbName,"timeout_min");
	    		values.addValue(((Number)map.get("timeout_avg")).doubleValue(),dbName,"timeout_avg");
	    		values.addValue(((Number)map.get("timeout_th95")).doubleValue(),dbName,"timeout_th95");
	    		values.addValue(((Number)map.get("timeout_th50")).doubleValue(),dbName,"timeout_th50");
	    	}
	    }
	    JFreeChart mChart = createBarChart("写入延迟对比图",  "性能指标","延迟时间", values);
//	    JFreeChart mChart= createXYLineChart("数据导入速度折线图", "导入次数","导入速度",mCollection);
	    saveAsFile(mChart,path, 2400, 800);
	}
	
	private static  JFreeChart createXYLineChart(String title,String category,String value, XYSeriesCollection dataset) {
	    JFreeChart mChart = ChartFactory.createXYLineChart(
	    		title,
	    		category,
	    		value,				
				dataset,
				PlotOrientation.VERTICAL,
				true, 
				true, 
				false);
//	      ChartPanel chartPanel = new ChartPanel( mChart);
//	      chartPanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
	      final XYPlot plot = mChart.getXYPlot();// 获取折线图plot对象
	      plot.setBackgroundPaint(new Color(240,240,240));
		  NumberAxis na= (NumberAxis)plot.getRangeAxis();
		  NumberAxis domainAxis = (NumberAxis)plot.getDomainAxis();
//		  na.setAutoTickUnitSelection(false);
		  na.setNumberFormatOverride(df);//设置轴坐标
		  domainAxis.setNumberFormatOverride(df);
	      XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer( );// 设置样式
	      renderer.setBaseShapesVisible(false);
	      renderer.setSeriesPaint(0,Color.RED);
		  renderer.setSeriesPaint(1,Color.GREEN);
		  renderer.setSeriesPaint(2,new Color(255,150,24));
		  renderer.setSeriesPaint(3,new Color(82,101,115));
	      plot.setRenderer( renderer ); 
	      return mChart;
	}
	private static  JFreeChart createBarChart(String title,String category,String value, CategoryDataset dataset) {   
	    JFreeChart chart = ChartFactory.createBarChart3D(title, // chart title   
	    			category, // domain axis label   
	    			value, // range axis label   
	                dataset, // data   
	                PlotOrientation.VERTICAL, // 图标方向   
	                true, // 是否显示legend   
	                true, // 是否显示tooltips   
	                false // 是否显示URLs   
	        );   
	    CategoryPlot plot = chart.getCategoryPlot();//设置图的高级属性 
	    plot.setBackgroundPaint(ChartColor.WHITE);
	    NumberAxis na= (NumberAxis)plot.getRangeAxis();
//	    na.setAutoTickUnitSelection(false);//设置小数点位数
	    na.setNumberFormatOverride(df);
//	    NumberTickUnit nt=new NumberTickUnit(1.22);
//	    na.setTickUnit(nt);
//	    plot.setRangeAxis(na);
	    BarRenderer3D renderer = new BarRenderer3D();//3D属性修改 
	    renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
	    renderer.setBaseItemLabelsVisible(true);
	    renderer.setItemLabelAnchorOffset(10);
	    renderer.setBasePositiveItemLabelPosition(new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12, TextAnchor.BASELINE_CENTER));
		renderer.setSeriesPaint(0,new Color(34,139,34));
		renderer.setSeriesPaint(1,new Color(154,205,50));
		renderer.setSeriesPaint(2,new Color(0,255,0));
		renderer.setSeriesPaint(3,new Color(127,255,212));
	    plot.setRenderer(renderer);//将修改后的属性值保存到图中 
	    return chart;   
	}
	public static void saveAsFile(JFreeChart chart, String outputPath,
			int weight, int height) {
			FileOutputStream out = null;
			try {
				File outFile = new File(outputPath);
				if (!outFile.getParentFile().exists()) {
					outFile.getParentFile().mkdirs();
				}
				out = new FileOutputStream(outputPath);
				// 保存为PNG
				ChartUtilities.writeChartAsPNG(out, chart, weight, height);
				// 保存为JPEG
				// ChartUtilities.writeChartAsJPEG(out, chart, weight, height);
				out.flush();
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
				if (out != null) {
					try {
						out.close();
					} catch (IOException e) {
					}
				}
			}
	}
	private static String getChartRootPath(String pngName){
    	String path = System.getProperty("user.dir");
		File dir=new File(path+"/charts");
		if(!dir.exists()){
			dir.mkdir();
		}
		return path+"/charts/"+pngName+"_"+System.currentTimeMillis()+".png";
	};
	/**
	 * 生成吞吐量图表，
	 * 共四张
	 * @param dbs
	 */
	public static void generateAllChartByDBList(List<String> dbs){
		try {
			generateThroughSizeXYLine(dbs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			generateThroughPointsXYLine(dbs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			generateThroughSizeBar(dbs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			generateThroughPointsBar(dbs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
//			generateThroughRequestsBXYLine(dbs);//FIXME 未实现
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
//			generateThroughRequestsBar(dbs);//FIXME 未实现
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			generateTimeoutBar(dbs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	/**
	 * 单性能测试直方图
	 * @param dbs
	 */
	private static void generateTimeoutBar(List<String> dbs) {
		DefaultCategoryDataset maxValues = new DefaultCategoryDataset();   
		DefaultCategoryDataset minValues = new DefaultCategoryDataset();   
		DefaultCategoryDataset avgValues = new DefaultCategoryDataset();   
		DefaultCategoryDataset th95Values = new DefaultCategoryDataset();   
		DefaultCategoryDataset th50Values = new DefaultCategoryDataset();   
		String sql="SELECT CASE (ttp.load_type) WHEN 1 THEN 'insert' WHEN 2 THEN 'random insert' WHEN 3 THEN 'update' WHEN 4 THEN 'simple read' WHEN 5 THEN 'analysis' END AS load_type, perform_batch.db db, timeout_th95 th95, timeout_th50 th50, timeout_max max, timeout_min min, timeout_avg avg FROM ts_timeout_perform ttp, ( SELECT batch.db db, max(tpb.id) id FROM ts_perform_batch tpb, ( SELECT max(id) id, target_db db FROM ts_load_batch tlb WHERE data_status = 1 GROUP BY target_db ) batch WHERE batch.id = tpb.load_batch_id GROUP BY tpb.load_batch_id ) perform_batch WHERE perform_batch.id = ttp.perform_batch_id AND ttp.load_type IN (1, 2, 3, 4, 5) GROUP BY ttp.load_type, perform_batch.db";
	    List<Map<String, Object>> list = BizDBUtils.selectListBySqlAndParam(sql);
	    if(list!=null&&list.size()>0){
	    	for(int i=0;i<list.size();i++){
	    		Map<String, Object> map = list.get(i);
	    		DecimalFormat df = new DecimalFormat("#.##");
	    		maxValues.addValue(Double.parseDouble(df.format(Math.log(((Number)map.get("max")).doubleValue()))),getDBName(map.get("db").toString()),map.get("load_type").toString());
	    		minValues.addValue(Double.parseDouble(df.format(Math.log(((Number)map.get("min")).doubleValue()))),getDBName(map.get("db").toString()),map.get("load_type").toString());
	    		avgValues.addValue(Double.parseDouble(df.format(Math.log(((Number)map.get("avg")).doubleValue()))),getDBName(map.get("db").toString()),map.get("load_type").toString());
	    		th95Values.addValue(Double.parseDouble(df.format(Math.log(((Number)map.get("th95")).doubleValue()))),getDBName(map.get("db").toString()),map.get("load_type").toString());
	    		th50Values.addValue(Double.parseDouble(df.format(Math.log(((Number)map.get("th50")).doubleValue()))),getDBName(map.get("db").toString()),map.get("load_type").toString());
	    	}
	    }
	    try {
			JFreeChart mChart = createBarChart("max_timeout_comparison",  "request_type","log timeout(us)", maxValues);
			String path=getChartRootPath("max_timeout_comparison_bar");
			saveAsFile(mChart,path, 1200, 800);
		} catch (Exception e) {
			e.printStackTrace();
		}
	    try {
	    	JFreeChart mChart = createBarChart("avg_timeout_comparison",  "request_type","log timeout(us)", avgValues);
	    	String path=getChartRootPath("avg_timeout_comparison_bar");
	    	saveAsFile(mChart,path, 1200, 800);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	    try {
	    	JFreeChart mChart = createBarChart("min_timeout_comparison",  "request_type","log timeout(us)", minValues);
	    	String path=getChartRootPath("min_timeout_comparison_bar");
	    	saveAsFile(mChart,path, 1200, 800);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	    try {
	    	JFreeChart mChart = createBarChart("95th_timeout_comparison",  "request_type","log timeout(us)", th95Values);
	    	String path=getChartRootPath("95th_timeout_comparison_bar");
	    	saveAsFile(mChart,path, 1200, 800);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	    try {
	    	JFreeChart mChart = createBarChart("50th_timeout_comparison",  "request_type","log timeout(us)", th50Values);
	    	String path=getChartRootPath("50th_timeout_comparison_bar");
	    	saveAsFile(mChart,path, 1200, 800);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}
	/**
	 * 请求吞吐量 直方图
	 * @param dbs
	 */
	private static void generateThroughRequestsBar(List<String> dbs) {
		DefaultCategoryDataset values = new DefaultCategoryDataset();   
		String sql="SELECT perform_batch.id, perform_batch.db db, sum(ttp.success_times) / ( sum(ttp.timeout_avg) / 1000000.0 ) AS avg, sum(ttp.success_times) / ( sum(ttp.timeout_min) / 1000000.0 ) AS max, sum(ttp.success_times) / ( sum(ttp.timeout_max) / 1000000.0 ) AS min FROM ts_timeout_perform ttp, ( SELECT batch.db db, max(tpb.id) id FROM ts_perform_batch tpb, ( SELECT max(id) id, target_db db FROM ts_load_batch tlb WHERE data_status = 1 GROUP BY target_db ) batch WHERE batch.id = tpb.load_batch_id GROUP BY tpb.load_batch_id ) perform_batch WHERE ttp.perform_batch_id = perform_batch.id AND ttp.load_type = 99 GROUP BY perform_batch.db";
	    List<Map<String, Object>> list = BizDBUtils.selectListBySqlAndParam(sql);
	    if(list!=null&&list.size()>0){
	    	for(int i=0;i<list.size();i++){
	    		Map<String, Object> map = list.get(i);
	    		values.addValue(((Number)map.get("max")).doubleValue(),getDBName(map.get("db").toString()),"max");
	    		values.addValue(((Number)map.get("min")).doubleValue(),getDBName(map.get("db").toString()),"min");
	    		values.addValue(((Number)map.get("avg")).doubleValue(),getDBName(map.get("db").toString()),"avg");
	    	}
	    }
	    JFreeChart mChart = createBarChart("数据库吞吐量对比图",  "性能指标","speed(requests/sec)", values);
	    String path=getChartRootPath("throughput_perform_requests_bar");
	    saveAsFile(mChart,path, 1200, 800);
	}
	/**
	 * 请求吞吐量折线图
	 * @param dbs
	 */
	private static void generateThroughRequestsBXYLine(List<String> dbs) {
		XYSeriesCollection mCollection = new XYSeriesCollection();
		for(String className:dbs){
			String sql="select rowid as row_num,timeout_avg value from ts_timeout_perform ttp where  ttp.load_type=99  and ttp.perform_batch_id=(select max(id) from ts_perform_batch tpb where tpb.load_batch_id =(select max(id) from ts_load_batch tlb where data_status=1  and target_db=?))";
			mCollection.addSeries(generateXYSeriesBySqlAndClassName(sql, className));
		}
	    JFreeChart mChart= createXYLineChart("数据库吞吐量折线对比图", "次数","speed(requests/sec)",mCollection);
	    String path=getChartRootPath("throughput_perform_requests_line");
	    saveAsFile(mChart,path, 24000, 800);
	}
	private static void generateThroughPointsBar(List<String> dbs) {
		DefaultCategoryDataset values = new DefaultCategoryDataset();   
		String sql="SELECT batch.db db, sum(load_points)/(sum(load_cost_time)/1000.0)  avg, max(pps) max, min(pps) min FROM ts_load_record tlr, ( SELECT max(id) id, target_db db FROM ts_load_batch tlb WHERE data_status = 1 GROUP BY target_db ) batch WHERE batch.id = tlr.load_batch_id GROUP BY load_batch_id";
	    List<Map<String, Object>> list = BizDBUtils.selectListBySqlAndParam(sql);
	    if(list!=null&&list.size()>0){
	    	for(int i=0;i<list.size();i++){
	    		Map<String, Object> map = list.get(i);
	    		values.addValue(Double.parseDouble(df.format(((Number)map.get("max")).doubleValue())),getDBName(map.get("db").toString()),"max");
	    		values.addValue(Double.parseDouble(df.format(((Number)map.get("min")).doubleValue())),getDBName(map.get("db").toString()),"min");
	    		values.addValue(Double.parseDouble(df.format(((Number)map.get("avg")).doubleValue())),getDBName(map.get("db").toString()),"avg");
	    	}
	    }
	    JFreeChart mChart = createBarChart("import_speed_comparison",  "性能指标","speed(points/s)", values);
	    String path=getChartRootPath("import_points_speed_comparison_line");
	    saveAsFile(mChart,path, 1200, 800);
	}
	private static DecimalFormat df=new DecimalFormat("####.##");
	private static void generateThroughSizeBar(List<String> dbs) {
		DefaultCategoryDataset values = new DefaultCategoryDataset();   
		String sql="SELECT batch.db db, sum(load_size/1024.0/1024)/(sum(load_cost_time)/1000.0) avg, max(sps) max, min(sps) min FROM ts_load_record tlr, ( SELECT max(id) id, target_db db FROM ts_load_batch tlb WHERE data_status = 1 GROUP BY target_db ) batch WHERE batch.id = tlr.load_batch_id GROUP BY load_batch_id";
	    List<Map<String, Object>> list = BizDBUtils.selectListBySqlAndParam(sql);
	    if(list!=null&&list.size()>0){
	    	for(int i=0;i<list.size();i++){
	    		Map<String, Object> map = list.get(i);
	    		values.addValue(Double.parseDouble(df.format(((Number)map.get("max")).doubleValue())),getDBName(map.get("db").toString()),"max");
	    		values.addValue(Double.parseDouble(df.format(((Number)map.get("min")).doubleValue())),getDBName(map.get("db").toString()),"min");
	    		values.addValue(Double.parseDouble(df.format(((Number)map.get("avg")).doubleValue())),getDBName(map.get("db").toString()),"avg");
	    	}
	    }
	    JFreeChart mChart = createBarChart("import_speed_comparison",  "性能指标","speed(MB/s)", values);
	    String path=getChartRootPath("import_size_speed_comparison_line");
	    saveAsFile(mChart,path, 1200, 800);
	}
	private static void generateThroughPointsXYLine(List<String> dbs) {
		XYSeriesCollection mCollection = new XYSeriesCollection();
		for(String className:dbs){
			String sql="select rowid as row_num,tlr.pps value from ts_load_record tlr where tlr.load_batch_id=(select max(id) from ts_load_batch tlb where data_status=1  and target_db=? )";
			mCollection.addSeries(generateXYSeriesBySqlAndClassName(sql, className));
		}
	    JFreeChart mChart= createXYLineChart("import_speed_comparison", "times","speed(points/sec)",mCollection);
	    String path=getChartRootPath("import_points_speed_comparison_line");
	    saveAsFile(mChart,path, 8000, 800);
	}
	private static void generateThroughSizeXYLine(List<String> dbs) {
		XYSeriesCollection mCollection = new XYSeriesCollection();
		for(String className:dbs){
			String sql="select rowid as row_num,tlr.sps value from ts_load_record tlr where tlr.load_batch_id=(select max(id) from ts_load_batch tlb where data_status=1  and target_db=? )";
			mCollection.addSeries(generateXYSeriesBySqlAndClassName(sql, className));
		}
	    JFreeChart mChart= createXYLineChart("import_speed_comparison", "times","speed(MB/sec)",mCollection);
	    String path=getChartRootPath("import_size_speed_comparison_line");
	    saveAsFile(mChart,path, 8000, 800);
	}
	public static XYSeries generateXYSeriesBySqlAndClassName(String sql,String className){
		XYSeries xySeries = new XYSeries(getDBName(className));
	    List<Map<String, Object>> influxdbList = BizDBUtils.selectListBySqlAndParam(sql,className);
	    DecimalFormat df = new DecimalFormat("####.##");
	    if(influxdbList!=null&&influxdbList.size()>0){
	    	for(int i=0;i<influxdbList.size();i++){
	    		Map<String, Object> map = influxdbList.get(i);
//	    		xySeries.add(((Number)map.get("row_num")).doubleValue(),((Number)map.get("value")).doubleValue());
	    		xySeries.add(i+1,Double.parseDouble(df.format(((Number)map.get("value")).doubleValue())));
	    	}
	    }
	    return xySeries;
	};
	private static String getDBName(String className){
		int index = className.lastIndexOf(".");
		String dbName = className.substring(index+1,className.length());
		if(dbName.toLowerCase().contains("tsfile")){
			dbName="IotDB";
		}
		return dbName.toLowerCase();
	}
	//生成图表程序
	public static void main(String[] args) {
		List<String> dbs=new ArrayList<String>();
		dbs.add("cn.edu.ruc.db.InfluxDB");
		dbs.add("cn.edu.ruc.db.TsfileDB");
		dbs.add("cn.edu.ruc.db.CassandraDB");
		dbs.add("cn.edu.ruc.db.OpentsDB");
		generateAllChartByDBList(dbs);	
	}	    
}

