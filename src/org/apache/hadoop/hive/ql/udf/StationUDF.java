package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 取字符串子串，以分隔符为标志,取固定个数的子串,如A,B,C,D -substring('A,B,C,D',',',3)--->A,B,C
 * @author jiaqiang
 * 2012.10.08
 */
public class StationUDF extends UDF {
	
	private Text rtn = new Text();
	
	
	/**
	 * 取子串
	 * @param src 源串
	 * @param delimiter 字符串的分隔符
	 * @param count 子串个数
	 * @param connector 返回子串的连接符
	 * @return
	 */
	public Text evaluate(final String station) {
		String s = station;
		
		int idx = station.indexOf(".");
		if (idx != -1) {
			s = station.substring(0,idx);
		}
		
		idx = s.indexOf("|");
		if (idx != -1) {
			s = s.substring(0,idx);
		}
		
		rtn.set(s);
		return rtn;
	}
}
