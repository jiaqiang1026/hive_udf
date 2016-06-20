package org.apache.hadoop.hive.ql.udf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 将搜索网站url中的搜索内容转化成中文
 * function: string get_words(string)
 * 参数： url or referer
 * 返回：搜索词或null
 * @author jiaqiang
 * 2014.06.10
 */
public final class UrlDecode2ChineseUDF extends UDF {
	
	public Text evaluate(final Text url) {
		if (url == null) {
			return null;
		}
		
		String url2 = url.toString().trim();
		String rtn = null;
		
		try {
			rtn = decode2chinese(url2);
		} catch (Exception ex) {
			rtn = null;
		}
	
		return rtn == null ? null : new Text(rtn);
	}
	
	//key:website value:website
	private static Map<String,String> searchWebsiteMap = new HashMap<String,String>();
	
	//匹配模式
	private String pattern = "^(\\w*[\u4e00-\u9fa5$]{1,}\\w*)*$";
	
	static {
		//加入7个搜索网站
		
		//百度
		searchWebsiteMap.put("baidu.com", "baidu.com");
		//谷歌
		searchWebsiteMap.put("google.com", "google.com");
		//必应
		searchWebsiteMap.put("bing.com", "bing.com");
		//搜搜
		searchWebsiteMap.put("www.soso.com", "www.soso.com");
		searchWebsiteMap.put("wap.soso.com", "wap.soso.com");
		
		//搜狗
		searchWebsiteMap.put("www.sogou.com", "www.sogou.com");
		//雅虎
		searchWebsiteMap.put("yahoo.com", "yahoo.com");
		//有道
		searchWebsiteMap.put("www.youdao.com", "www.youdao.com");
		//360搜索
		searchWebsiteMap.put("www.so.com", "www.so.com");
	}
	
	/**
	 * 
	 * @param url
	 * @param charset
	 * @return
	 * @throws CharacterCodingException
	 * @throws UnsupportedEncodingException
	 */
	public String decode2chinese(String url) {
		if (url == null) {
			return null;
		}
		
		String url2 = url.trim(), sWebsite = null;
		if (url2.length() == 0) {
			return null;
		}
		
		//只处理搜索网站
		for (String website : searchWebsiteMap.keySet()) {
			if (url2.indexOf(website) != -1) { //是搜索网站
				sWebsite = website;
				break;
			}
		}
		
		if (sWebsite == null) { //非搜索网站
			return null;
		}
		
		//获取查询参数的下标
		int idx = url2.indexOf("?");
		if (idx == -1) { //无查询
			return null;
		}
	
		//查询参数
		String query = url2.substring(idx+1);
		if (query.length() == 0) { 
			return null;
		}
		
		return decode(sWebsite, query);
	}
	
	/**
	 * 解析url中的查询 
	 * @param searchWebsite 搜索网站,如baidu
	 * @param query url中的查询部分
	 * @return 
	 */
	private String decode(String searchWebsite, String query) {
		if (query == null || query.trim().length() == 0) {
			return null;
		}
		
		//字符集
		String charset = null;
		if (query.contains("gbk") || query.contains("GBK") || query.contains("gb2312") || query.contains("GB23123")) {
			charset = "gbk";
		}
		if (query.contains("utf-8") || query.contains("utf8") || query.contains("UTF-8") || query.contains("UTF8")) {
			charset = "utf8";
		}
		
		if (charset == null) { //根据搜索网站来指定字符集，可能会产生乱码
			if ("baidu.com".equals(searchWebsite)) {
				charset = "utf-8";
			} else if ("google.com".equals(searchWebsite)) {
				charset = "utf-8";
			} else if ("bing.com".equals(searchWebsite)) {
				charset = "utf-8";
			} else if ("www.soso.com".equals(searchWebsite)) {
				charset = "gbk";
			}  else if ("wap.soso.com".equals(searchWebsite)) {
				charset = "utf-8";
			} else if ("www.sogou.com".equals(searchWebsite)) {
				charset = "gbk";
			} else if ("yahoo.com".equals(searchWebsite)) {
				charset = "utf-8";
			} else if ("www.youdao.com".equals(searchWebsite)) {
				charset = "utf-8";
			} 
		}
	
		try {
			query = URLDecoder.decode(query, charset);
		} catch (UnsupportedEncodingException e) {
			return null;
		}
		
		//替换无用字符
		query = query.replace(" ", "").replace(",", "").replace("，", "").replace(".", "").replace("。", "")
		        .replace("?", "").replace("？", "").replace("!", "").replace("！", "").replace("/", "")
		        .replace("-", "").replace("——", "");
		
		String queryValue = "", value = null;
		String[] arr = query.split("&");
		int idx = -1;
		
		for (String pair : arr) {
			idx = pair.indexOf("=");
			if (idx != -1) {
				try {
					value = pair.substring(idx+1);
					if (value.matches(pattern)) {
						queryValue += value;
					}
				} catch (Exception ex) {
					continue;
				}
			}
		}
		
		return queryValue.length() == 0 ? null : queryValue;
	}
}
