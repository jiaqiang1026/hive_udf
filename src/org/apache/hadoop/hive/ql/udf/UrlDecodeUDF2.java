package org.apache.hadoop.hive.ql.udf;

import java.net.URLDecoder;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 对url进行decode
 * 对应hive 函数是签名:
 * 1) 签名 String decode_url(String url,[int n,String charset])
 * @author jiaqiang	
 * 2014.08.01
 */
public class UrlDecodeUDF2 extends UDF {
	
	//decode一次,utf-8解码
	public String evaluate(final String str) {
		return evaluate(str, 1, "utf-8");
	}

	//decode n次,utf-8解码
	public String evaluate(final String str, Integer n) {
		return evaluate(str,n,"utf-8");
	}
	
	//decode n次,指定charset解码
	public String evaluate(final String str, Integer n, String charset) {
		if (str == null || str.trim().length() == 0) {
			return null;
		}
		
		String s = str.trim();
		n = (n <= 0 ? 1 : n);
		
		for (int i = 1; i <= n; i++) {
			try {
				s = URLDecoder.decode(s, charset);
			} catch (Exception e) {
				s = null;
				break;
			}
		}
		
		return s;
	}
}
