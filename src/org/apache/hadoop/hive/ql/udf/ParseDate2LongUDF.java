package org.apache.hadoop.hive.ql.udf;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;

/**
 * 将'yyyy-MM-dd HH:mm:ss'格式的日期转换成长整型
 * @author jiaqiang
 * 2012.11.14
 */
public class ParseDate2LongUDF extends UDF {

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private LongWritable result = new LongWritable();
	
	private StringBuffer buff = new StringBuffer(200);
	
	private final Calendar c = Calendar.getInstance();
	
	public LongWritable evaluate(String date) {
		if (date == null) {
			return null;
		}
		
		String s = date.toString().trim();
		if (s.length() == 0) {
			return null;
		}
		
		buff.setLength(0);
		try {
			Date d = sdf.parse(date);
			c.setTime(d);
			buff.append(c.get(Calendar.YEAR)).append((c.get(Calendar.MONTH)+1)).append(c.get(Calendar.DAY_OF_MONTH))
			.append(c.get(Calendar.HOUR_OF_DAY)).append(c.get(Calendar.MINUTE)).append(c.get(Calendar.SECOND));
			
			result.set(Long.parseLong(buff.toString()));
		} catch (Exception e) {
			return null;
		}
		
		return result;
	}
}
