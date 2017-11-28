package cn.edu.ruc.biz.chart;

import java.awt.Font;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import cn.edu.ruc.biz.Core;
import cn.edu.ruc.biz.db.BizDBUtils;

public class Test {
	public static void main(String[] args) {
		DecimalFormat df = new DecimalFormat("#.##");
		double a=13223235.6698D;
		String format = df.format(a);
		System.out.println(Double.parseDouble(df.format(a)));
		long b=999994444L;
		DecimalFormat df1 = new DecimalFormat("####.##");
		System.out.println(df1.format(a));
	}
}

