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
import org.apache.hadoop.io.IntWritable;

/**
 * 获取暗拍时的结算调节价
 * 函数：int get_settle_adjust_price(string) 
 * 参数：adx平台名称(sid, 如tanx,bes,allyes等),如果无该adx，则返回默认值1(单位为分)
 * 返回值:平台对应的是暗拍时的结算调节价
 * @author jiaqiang
 * 2014.02.19
 */
public class GetSettleAdjustPriceUDF extends UDF {
	
	//佣金比例配置文件
	private final static String confPath = "/user/hive/warehouse/commission/conf.txt";
	
	//key:adx value:结算调节价
	private final static Map<String,Integer> adjustPriceMap = new ConcurrentHashMap<String,Integer>();
	
	//平台对应的暗拍结算调节价
	private IntWritable adjustPrice = new IntWritable(1);
	
	static {
		boolean succ = load();
		if (!succ) {
			System.out.println("加载暗拍置文件["+confPath+"]失败");
		}
	}
	
	public IntWritable evaluate(String adx) {
		if (adx == null || adx.trim().length() == 0) {
			return adjustPrice;
		}
		
		adx = adx.trim();
		Integer p = adjustPriceMap.get(adx);
		if (p != null) {
			adjustPrice.set(p);
		}
		
		return adjustPrice;
	}
	
	/**
	 * 从hdfs中加载佣金比例配置文件
	 * @return
	 */
	private static synchronized boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(confPath), conf);
			in = fs.open(new Path(confPath));
			
			String line = null, adx = null;
			Integer adjustPrice = 1;
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				if (arr.length >= 3) { //有效值
					try {
						adx = arr[0];
						adjustPrice = Integer.parseInt(arr[2]);
						adjustPriceMap.put(adx, adjustPrice);
					} catch (Exception ex) {
						ex.printStackTrace();
						adjustPriceMap.put(adx, 1);
					}
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
