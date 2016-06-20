package org.apache.hadoop.hive.ql.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * ����ת�ַ���
 * @author jiaqiang
 * 2012.09.12
 */
public class ToCharUDF extends UDF {
	
	//Ҫת�ɵ�Ĭ�����ڸ�ʽ
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	//���������ʽ
	private SimpleDateFormat valueSDF = new SimpleDateFormat("yyyyMMddHHmmss");
	
	public String evaluate(String date) {
		if (date == null || date.trim().length() == 0) {
			return null;
		}
		
		date = date.trim();
		String rtn = null; 
		
		try {
			Date d = valueSDF.parse(date);
			rtn = sdf.format(d);
		} catch (ParseException e) {
			return null;
		}
		
		return rtn;
	}
	
	public String evaluate(String date, String pattern) {
		if (date == null || date.trim().length() == 0) {
			return null;
		}
		date = date.trim();
		String rtn = null;
		
		//�Զ������ڸ�ʽ
		if (pattern != null && pattern.trim().length() != 0) {
			//ͳһģʽ��ʾ
			pattern = pattern.trim().replace("YYYY", "yyyy")
					                .replace("mm", "MM")
					                .replace("DD", "dd")
									.replace("hh", "HH")
									.replace("mi", "mm")
									.replace("SS", "ss");
			
			SimpleDateFormat sdf = new SimpleDateFormat(pattern);
			try {
				Date d = valueSDF.parse(date);
				rtn = sdf.format(d);
			} catch (ParseException e) {
				return null;
			}
		} else {
			rtn = evaluate(date);
		}
		
		return rtn;
	}
	
	public static void main(String[] args) {
		System.out.println(System.currentTimeMillis());
	}

}
