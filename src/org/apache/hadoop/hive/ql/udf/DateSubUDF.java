package org.apache.hadoop.hive.ql.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * �����������,���ڸ�ʽ��yyyyMMdd����20120801
 * @author jiaqiang
 * 2012.08.17
 */ 
public class DateSubUDF extends UDF {
		 
	private Calendar c = Calendar.getInstance();
	
	private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
	
	/**
	 * yyyyMMdd��ʽ������
	 * @param date
	 * @param amount Ҫ��������
	 * @return
	 */ 
	public Integer evaluate(Integer date, Integer days) {
		String s = date + "";
		
		try {
			c.setTime(sdf2.parse(s));
			c.add(Calendar.DAY_OF_MONTH, -(days));
		} catch (ParseException e) {
			return null;
		}
		
		return Integer.parseInt(sdf2.format(c.getTime()));
	}
	
}
