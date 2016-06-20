package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IOUtils;

/**
 * 判断某平台是否为exchange平台
 * 函数签名 boolean is_adx(string)
 * param:sid
 * return sid是否是exchange平台
 * @author jiaqiang
 * 2014-02-26
 */
public final class IsAdxUDF extends UDF {
	
	//平台维表
	private final static String dimSource = "/user/hive/warehouse/dim_source/dim_source.txt";
	
	//key:sid value:flag 0:no 1:yes
	private static Map<String, Integer> adxFlagMap = new ConcurrentHashMap<String, Integer>();
	
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
	public boolean evaluate(final String sid) {
		if (!initSucc) { //初使化失败
			return false;
		}
		
		if (sid == null || sid.trim().length() == 0) {
			return false;
		}
		
		Integer adxFlag = adxFlagMap.get(sid.trim());
		
		return (adxFlag == null ? false : (adxFlag == 0 ? false : true));
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
			String line = null,sid = null;
			Integer adxFlag = 0;
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				if (arr.length == 4) { //有效值
					try {
						adxFlag = Integer.parseInt(arr[3]);
					} catch (Exception ex) {
						adxFlag = 0;
					}
					
					adxFlagMap.put(arr[0].trim(),adxFlag);
					adxFlagMap.put(arr[1].trim(),adxFlag);
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
