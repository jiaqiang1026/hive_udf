package org.apache.hadoop.hive.ql.udf;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MapUDF extends UDF {

	private Text text = new Text();
	
	/**
	 * ȡ��HQL������Ϊ Map��key����Ӧ��value
	 * 
	 * @return Text
	 * 
	 */
	public Text evaluate(Map<String, String> entry, String key) {
		try {
			if (null == entry) {
				return null;
			}
			if ("".equals(key) || null == key) {
				return null;
			}
			if (entry.containsKey(key)) {
				text.set(entry.get(key));
				return text;
			} else {
				return null;
			}
		} catch (Exception ex) {
			return null;
		}
	}
}
