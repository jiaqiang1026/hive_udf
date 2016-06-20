package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * ½âÎöfloatÖµº¯Êý
 * @author jiaqiang
 * 2012.11.10
 */
public class ParseFloatUDF extends UDF {
	
	public Float evaluate(Object obj) {
		if (obj == null) {
			return null;
		}
		
		String s = obj.toString().trim();
		if (s.length() == 0) {
			return null;
		}
		
		Float f = null;
		try {
			f = Float.parseFloat(s);
		} catch (Exception e) {
			f = null;
		}
		
		return f;
	}
}
