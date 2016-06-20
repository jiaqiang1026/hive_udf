package org.apache.hadoop.hive.ql.udf;

import java.net.URLDecoder;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * URL解析UDF,只截取以http://|https://开头，并且如果url包括www,则截取到顶级域名一级如http://www.sina.com的后一级目录
 * 如http://www.sina.com/sport
 * 如果url不包括www,则截取到一级域名,如http://news.sina.com/mil/aaa/bbb-->http://news.sina.com
 * @author jiaqiang
 * 2012.09.14
 */
public class GetUsefulDomainUDF extends UDF {
	
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
		
		//删除前缀
		url = url.replace(prefix, "");
		return parse(prefix, url);
	}
	
	public String evaluate(String url, boolean decode) {
		if (url == null || url.trim().length() == 0) {
			return null;
		}
		
		url = url.trim();
		
		if (decode) {
			url = URLDecoder.decode(url);
		}
		
		String prefix = null;
		
		//只处理以http://或https://前头的url
		if (url.startsWith("http://")) {
			prefix = "http://";
		} else if (url.startsWith("https://")){
			prefix = "https://";
		} else {
			return null;
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
		if (idx1 != -1) { //不包括斜线
			postfix = postfix.substring(0,idx1);
		}
	
		return prefix + postfix;
	}	
	
	public static void main(String[] args) {
		String url = "http://www.sina.com/sport/aaa/bbb?ad=2";
		GetUsefulDomainUDF udf = new GetUsefulDomainUDF();
		
		System.out.println(udf.evaluate(url));
	}
}
