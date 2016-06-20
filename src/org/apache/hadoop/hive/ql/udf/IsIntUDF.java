package org.apache.hadoop.hive.ql.udf;

import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 判断参数是否为整型
 * @author jiaqiang
 * 2014.03.19
 */
public class IsIntUDF extends UDF {
	
	public boolean evaluate(String v) {
		if (v == null) {
			return false;
		}
		
		v = v.trim();
		if (v.length() == 0) {
			return false;
		}
		
		boolean succ = true;
		
		try {
			Pattern p = Pattern.compile("^[-\\+]?[\\d]*$");
			succ = p.matcher(v).matches();
		} catch (Exception ex) {
			succ = false;
		}
		
		return succ;
	}
}
