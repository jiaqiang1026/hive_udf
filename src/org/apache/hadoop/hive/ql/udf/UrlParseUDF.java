package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * URL解析UDF,只截取以http://|https://开头，并且如果url包括www,则截取到顶级域名一级如http://www.sina.com的后一级目录
 * 如http://www.sina.com/sport
 * 如果url不包括www,则截取到一级域名,如http://news.sina.com/mil/aaa/bbb-->http://news.sina.com
 * @author jiaqiang
 * 2012.09.14
 */
public class UrlParseUDF extends UDF {
	
	public String evaluate(String url) {
		if (url == null || url.trim().length() == 0) {
			return null;
		}
		
		url = url.trim();
		String prefix = null;
		
		//只处理以http://或https://前头的url
		if (url.startsWith("http://")) {
			prefix = "http://";
		} else if (url.startsWith("https://")){
			prefix = "https://";
		} else {
			return null;
		}
		
		//去除请求参数
		int idx = url.indexOf("?");
		if (idx != -1) {
			url = url.substring(0,idx);
		}
		
		//删除前缀
		url = url.replace(prefix, "");
		return parse(prefix, url);
	}
	
	//解析
	private String parse(String prefix, String url) {
		if (url.length() == 0) {
			return null;
		}
		
		//后缀
		String postfix = url;
		
		//第一斜线下标
		int idx1 = postfix.indexOf("/");
		if (idx1 == -1) { //不包括斜线
			return prefix + postfix;
		}
		
		if (postfix.startsWith("www")) { //以www开头
			try {
				String t = postfix.substring(idx1+1);
				if (t.length() == 0) {
					postfix = postfix.substring(0, idx1);
				} else {
					//第二个斜线下标
					int idx2 = t.indexOf("/");
					if (idx2 != -1) {
						postfix = postfix.substring(0,idx1+idx2+1);
					} else {
						postfix = postfix.substring(0,idx1+t.length()+1);	
					}
				}
			} catch (IndexOutOfBoundsException ex) {
				postfix = postfix.substring(0,idx1);
			}
		} else { //不以www开头
			postfix = postfix.substring(0, idx1);
		}
		
		return prefix + postfix;
	}
	
	public static void main(String[] args) {
		String url = "http://www.818.com/DrugDetails_10780520.shtml";
		UrlParseUDF udf = new UrlParseUDF();
		
		System.out.println(udf.evaluate(url));
	}
}
