package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;

/**
 * 获取暗拍时,平台广告主对应的结算系数
 * 函数：float get_settle_factor(string,string) 
 * 参数1:adx平台名称(sid, 如tanx,bes,allyes等)
 * 参数2:广告主id(adv_id)
 * 返回值:该平台上该广告主的结算系数，如果查询不到返回默认值1.1
 * @author jiaqiang
 * 2014.05.20
 */
public class GetSettlementFactorUDF extends UDF {
	
	//配置文件
	private final static String confPath = "/user/hive/warehouse/adx_adv_discount/adx_adv_discount.txt";
	
	//key:adx value:map<adv_id,settlement factor>
	private final static Map<String,Map<String,Float>> sfMap = new ConcurrentHashMap<String,Map<String,Float>>();
	
	//default settlement factor
	private FloatWritable sf = new FloatWritable(1.1f);
	
	static {
		boolean succ = load();
		if (!succ) {
			System.out.println("加载暗拍配置文件["+confPath+"]失败");
		}
	}
	
	/**
	 * 获取平台广告主对应的结算系数
	 * @param adx    平台id
	 * @param advId  广告主id
	 * @return 有则返回无则返回默认值1.1
	 */
	public FloatWritable evaluate(String adx, String advId) {
		if (adx == null || adx.trim().length() == 0 || advId == null || advId.trim().length() == 0) {
			return sf;
		}
		
		//获取平台
		Map<String,Float> advMap = sfMap.get(adx.trim());
		if (advMap == null) { //无平台
			return sf;
		}
		
		//获取广告主
		Float v = advMap.get(advId.trim());
		if (v == null) {
			return sf;
		}
		
		sf.set(v.floatValue());
		
		return sf;
	}
	
	/**
	 * 从hdfs中加载平台广告主配置的结算系数
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
			
			String line = null, adx = null, advId = null;
			Float sf = 1.1f;
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				if (arr.length >= 3) { //有效值
					try {
						adx = arr[0].trim();
						advId = arr[1].trim();
						sf = Float.parseFloat(arr[2].trim());
						
						Map<String,Float> map = sfMap.get(adx);
						if (map == null) {
							map = new HashMap<String,Float>();
						}
						map.put(advId, sf);
						sfMap.put(adx, map);
					} catch (Exception ex) {
					}
				}
			}
		} catch (IOException e) {
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		return succ;
	}
	
}
