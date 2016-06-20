package org.apache.hadoop.hive.ql.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 获取当月最一天的日期
 * @author jiaqiang
 * 2016.02.17
 */
public class FirstDayOfMonthUDF extends UDF {
		 
	private final Calendar c = Calendar.getInstance();
	
	private final ConcurrentHashMap<String,SimpleDateFormat> formatMap = new ConcurrentHashMap<String,SimpleDateFormat>();
	
	public String evaluate(String currDate, String pattern) {
		SimpleDateFormat f = formatMap.get(pattern);
		if (f == null) {
			f = new SimpleDateFormat(pattern);
			formatMap.put(pattern, f);
		}
		
		try {
			c.setTime(f.parse(currDate));
			c.set(Calendar.DAY_OF_MONTH, 1);
		} catch (ParseException e) {
			return null;
		}
		
		return f.format(c.getTime());
	}
}
