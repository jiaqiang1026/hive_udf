package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 截取域名 
 * @author jiaqiang
 * 2015.04.20
 */
public class UrlParseUDF2 extends UDF {
	
	public String evaluate(String url) {
		if (url == null || url.trim().length() == 0) {
			return null;
		}
		
		String rtn = url.trim();
		
		//去除协议前缀
		rtn = rtn.replace("http://", "").replace("https://", "").replace("www.", "");
		
		int idx = rtn.indexOf("/");
		if (idx != -1) {
			rtn = rtn.substring(0,idx);
		}
		
		return rtn;
	}
}
