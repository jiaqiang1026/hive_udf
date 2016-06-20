package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

/**
 * 判断某ip是否为网吧ip
 * 基于网吧ip库 
 * @author jiaqiang
 * 2016.01.28
 */
public class IsNetBarIPUDF extends UDF {
	
	private final static String ipPath = "/user/rtb/ip/netbar_ip_c.txt";
	
	private static boolean initSucc = false;
	
	//key:hashcode(ip), value:ip
	private static ConcurrentHashMap<String,String> ipMap = new ConcurrentHashMap<String, String>();
	
	static {
		boolean succ = load();
		System.out.println("加载量[" + ipMap.size() + "]");
		if (succ) {
			initSucc = true;
		}
	}
	
	public Boolean evaluate(final Text ip) {
		if (!initSucc) { //初使化失败
			return null;
		}
		
		if (ip == null) {
			return false;
		}
		
		final String ipStr = ip.toString().trim();
		if (ipStr.length() == 0) {
			return false;
		}
		
		return ipMap.containsKey(ipStr);
	}
		
	/*
	 * 加载ip库
	 */
	private static synchronized boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(ipPath), conf);
			in = fs.open(new Path(ipPath));
			String ip = null;
			
			while ((ip = in.readLine()) != null) {
				ip = ip.trim();
				ipMap.put(ip, ip);
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
