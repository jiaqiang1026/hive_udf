package org.apache.hadoop.hive.ql.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 日期加减函数   
 * @author jiaqiang
 * 2015.11.04
 */ 
public class DateAddUDF2 extends UDF {
		 
	private final Calendar c = Calendar.getInstance();
	
	/**
	 * 对输入日期进行days的加|减,日期格式为pattern
	 * @param date     日期
	 * @param days     加|减 天数
	 * @param pattern  日期pattern
	 * @return
	 */
	public Integer evaluate(Integer date, Integer days, String pattern) {
		String r = evaluate(date+"", days, pattern);
		
		return r == null ? null : Integer.parseInt(r);
	}
	
	/**
	 * 对输入日期进行days的加|减,日期格式为pattern
	 * @param date     日期
	 * @param days     加|减 天数
	 * @param pattern  日期pattern
	 * @return
	 */
	public String evaluate(String date, Integer days, String pattern) {
		SimpleDateFormat f = new SimpleDateFormat(pattern);
		
		try {
			c.setTime(f.parse(date));
			c.add(Calendar.DAY_OF_MONTH, days);
		} catch (ParseException e) {
			return null;
		}
		
		return f.format(c.getTime());
	}
}
