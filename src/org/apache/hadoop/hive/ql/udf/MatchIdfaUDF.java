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
 * 匹配idfa(idfa_md5)函数
 * 读取离线idfa(idfa_md5)文件，与参数idfa(idfa_md5)比较，相同返回true,不相同返回false
 * 函数签名：boolean match_idfa()
 * @author jiaqiang
 * 2015.08.26
 */
public final class MatchIdfaUDF extends UDF {
	
	//ip区域映射文件路径
	private static String idfaPath = "/user/rtb/idfa/$DATE/idfa.txt";
	
	//初使化标识
	private static boolean init = false;
	
	private static ConcurrentHashMap<String,Integer> idfaMap = new ConcurrentHashMap<String,Integer>(2000,0.95f);

	/**
	 * 匹配idfa
	 */
	public boolean evaluate(String day, final Text idfa) {
		if (!init) { //未始初化,替换日期变量并加载数据 
			idfaPath = idfaPath.replace("$DATE", day);
			if (!load()) { //load failed
				return false;
			}
			init = true;
		}
		
		if (idfa == null) {
			return false;
		}
		
		String idfaStr = idfa.toString().trim();
		if (idfaStr.length() == 0) { //空串不做匹配
			return false;
		}
	
		return idfaMap.containsKey(idfaStr);
	}
	

	/**
	 * 从hdfs中加载idfa(idfa_md5)文件
	 * @return
	 */
	private static boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(idfaPath), conf);
			in = fs.open(new Path(idfaPath));
			String line = null;
			
			while ((line = in.readLine()) != null) {
				if (line.trim().length() != 0) {
					idfaMap.put(line.trim(), 0);
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
