package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 取字符串子串，以分隔符为标志,取固定个数的子串,如A,B,C,D -substring('A,B,C,D',',',3)--->A,B,C
 * @author jiaqiang
 * 2012.10.08
 */
public class SubStringUDF extends UDF {
	
	private Text rtn = new Text();
	
	
	/**
	 * 取子串
	 * @param src 源串
	 * @param delimiter 字符串的分隔符
	 * @param count 子串个数
	 * @param connector 返回子串的连接符
	 * @return
	 */
	public Text evaluate(String src, String delimiter, int fromIdx, int endIdx, String connector) {
		if (src == null) {
			return null;
		}
		
		String str = src.toString().trim();
		if (str.length() == 0) {
			return null;
		}
		connector = (connector == null ? "," : connector);
		try {
			String[] arr = str.split(delimiter);
			String t = "";
				
			for (int i = fromIdx; i < endIdx; i++) {
				t += (i != endIdx-1 ? arr[i]+connector : arr[i]);
			}
			
			rtn.set(t);
		} catch (Exception ex) {
			return null;
		}
		
		return rtn;
	}
	
	
	/**
	 * 取子串
	 * @param src 源串
	 * @param delimiter 字符串的分隔符
	 * @param count 子串个数
	 * @param connector 返回子串的连接符
	 * @return
	 */
	public Text evaluate(String src, String delimiter, int count, String connector) {
		if (src == null) {
			return null;
		}
		
		String str = src.toString().trim();
		if (str.length() == 0) {
			return null;
		}
		connector = (connector == null ? "," : connector);
		try {
			String[] arr = str.split(delimiter);
			String t = "";

			if (count > arr.length) {
				for (int i = 0,len=arr.length; i < len; i++) {
					t += (i != len-1 ? arr[i]+delimiter : arr[i]);
				}
			} else {
				for (int i = 0; i < count; i++) {
					t += (i != count-1 ? arr[i]+connector : arr[i]);
				}
			}
			
			rtn.set(t);
		} catch (Exception ex) {
			return null;
		}
		
		return rtn;
	}
	
	/**
	 * 取子串
	 * @param src 源串
	 * @param delimiter 字符串的分隔符
	 * @param count 子串个数
	 * @param connector 返回子串的连接符
	 * @return
	 */
	public Text evaluate(String src, String delimiter, int count) {
		return evaluate(src,delimiter,count,delimiter);
	}
	
	public static void main(String[] args) {
		String ip = "127.0.0.1";
		//System.out.println(ip.split("[.]").length);
		SubStringUDF u = new SubStringUDF();
		System.out.println(u.evaluate(ip,"\\.",3,"."));
		
	}
}
