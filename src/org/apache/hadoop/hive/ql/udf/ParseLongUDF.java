package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * ½âÎöLongÖµº¯Êý
 * @author jiaqiang
 * 2012.11.10
 */
public class ParseLongUDF extends UDF {
	
	public Long evaluate(Object obj) {
		if (obj == null) {
			return null;
		}
		
		String s = obj.toString().trim();
		if (s.length() == 0) {
			return null;
		}
		
		Long f = null;
		try {
			f = Long.parseLong(s);
		} catch (Exception e) {
			f = null;
		}
		
		return f;
	}
}
