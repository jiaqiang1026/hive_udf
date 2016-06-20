package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IOUtils;

/**
 * 根据url|referer判断该网站是否是搜索引擎网站(如baidu，google,soso etc)
 * 判断依据是查询自定义搜索引擎网站维表
 * 函数签名 boolean is_search_website(string)
 * param:url|referer
 * return true:是search website, false:不同search website
 * @author jiaqiang
 * 2014-06-10
 */
public final class IsSearchWebsiteUDF extends UDF {
	
	//搜索网站维表
	private final static String dimSource = "/user/hive/warehouse/dim_search_website/dim_search_website.txt";
	
	//domain map
	private static Map<String,String> domainMap = new HashMap<String,String>();
	
	//初使化成功标识
	private static boolean initSucc = false;
	
	static {
		boolean succ = load();
		if (succ) {
			initSucc = true;
		}
	}
	
	/**
	 * 判断 sid 是否是exchange平台
	 * @param sid
	 * @return
	 */
	public boolean evaluate(String url) {
		if (url == null || url.trim().length() == 0) {
			return false;
		}
		
		if (!initSucc) { //初使化失败
			return false;
		}
		
		boolean succ = false;
		String url2 = url.trim();
		
		//只处理搜索网站
		for (String website : domainMap.keySet()) {
			if (url2.indexOf(website) != -1) { //是搜索网站
				succ = true;
				break;
			}
		}
		
		return succ;
	}
	
	
	/**
	 * 从hdfs中加载数据
	 * @return
	 */
	private static synchronized boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(dimSource), conf);
			in = fs.open(new Path(dimSource));
			String line = null,domain = null;
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				if (arr.length >= 3) { //有效值
					domain = arr[2].trim();
					domainMap.put(domain,domain);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		return succ;
	}
}
