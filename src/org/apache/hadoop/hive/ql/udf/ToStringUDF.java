package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * ×ª×Ö·û´®º¯Êý
 * @author jiaqiang
 * 2012.11.13
 */
public class ToStringUDF extends UDF {

	private Text result = new Text();
	
	public Text evaluate(String obj) {
		if (obj == null) {
			return null;
		}
		
		try {
			result.set(obj.toString());
		} catch (Exception e) {
			return null;
		}
		
		return result;
	}
	
}
