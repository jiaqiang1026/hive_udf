package org.apache.hadoop.hive.ql.udf;

import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 判断标签是否为有效标签
 * boolean is_valid_tag(tag)  | boolean is_valid_tag(tag,split)
 * @author jiaqiang
 * 2014.04.03
 */
public class IsValidTagUDF extends UDF {
	
	public boolean evaluate(String v) {
		return evaluate(v,"_");
	}
	
	public boolean evaluate(String tag, String regex) {
		if (tag == null) {
			return false;
		}
		
		tag = tag.trim();
		if (tag.length() == 0) { //空串情况
			return true;
		}
		
		boolean succ = true;
		
		try {
			String[] arr = tag.split(regex);
			for (String v : arr) {
				Pattern p = Pattern.compile("^[-\\+]?[\\d]*$");
				succ = succ && p.matcher(v).matches();
				if (!succ) {
					break;
				}
			}
		} catch (Exception ex) {
			succ = false;
		}
		
		return succ;
	}
}
