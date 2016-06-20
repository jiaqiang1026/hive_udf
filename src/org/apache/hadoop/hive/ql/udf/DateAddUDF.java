package org.apache.hadoop.hive.ql.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 日期相加函数,日期格式是yyyyMMdd，如20120801
 * @author jiaqiang
 * 2012.08.20
 */ 
public class DateAddUDF extends UDF {
		 
	private final Calendar c = Calendar.getInstance();
	
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
	
	/**
	 * yyyyMMdd格式的日期
	 * @param date
	 * @param amount 要减的天数
	 * @return
	 */ 
	public Integer evaluate(Integer date, Integer days) {
		String s = date + "";
		
		try {
			c.setTime(sdf2.parse(s));
			c.add(Calendar.DAY_OF_MONTH, days);
		} catch (ParseException e) {
			return null;
		}
		
		return Integer.parseInt(sdf2.format(c.getTime()));
	}
	
}
