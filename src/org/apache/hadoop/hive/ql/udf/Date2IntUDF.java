package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * 将yyyy-MM-dd HH:mi:ss格式的日期替换成yyyyMMddHHmiss,hive函数date2int('yyyy-MM-dd HH:mi:ss'),
 * @author tangjunliang
 * 2012.12.24
 */
public class Date2IntUDF extends UDF {
	
	private LongWritable lw = new LongWritable();
	
	public LongWritable evaluate(TimestampWritable tw) {
		if (null == tw) {
			return null;
		}
		
		String time = tw.toString();
		if ((time = time.trim()).length() == 0) {
			return null;
		}
		
		try {
			time = time.replace("-","").replace(":","").replace(" ", "");
			lw.set(Long.parseLong(time));
		} catch (Exception e) {
			return null;
		}
		
		return lw;
	}
}
