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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;

/**
 * 智云众收取的的各平台的佣金比例
 * 函数：float get_commision_rate(string,String) 
 * 参数1：adx平台名称(sid, 如tanx,bes,allyes等),如果无该adx，则返回默认值0.1
 * 参数2：广告主id
 * 返回值: 平台无配置时，返回默认值0.1;广告主无配置时，返回平台对应的佣金比例;平台广告主配置时，返回广告主对应佣金比例
 * @author jiaqiang
 * 2014.02.19
 */
public class GetCommissionRateUDF extends UDF {
	
	//adx平台对应佣金比例配置文件
	private final static String adxConfPath = "/user/hive/warehouse/commission/adx_conf.txt";
	
	//adx平台广告主对应佣金比例配置文件
	private final static String advConfPath = "/user/hive/warehouse/commission/adv_conf.txt";
	
	//key:adx value:cr
	private final static Map<String,Float> adxCommissionRateMap = new ConcurrentHashMap<String,Float>();
	
	//key:adx value:Map<String,Float> [key:advId,value:cr]
	private final static Map<String,Map<String,Float>> advCommissionRateMap = new ConcurrentHashMap<String,Map<String,Float>>();
		
	//返回的佣金比例,默认值为0.1
	private FloatWritable commissionRate = new FloatWritable(0.1f);
	
	static {
		boolean succ = load();
		if (!succ) {
			System.out.println("加载暗拍配置文件["+adxConfPath+","+advConfPath + "]失败");
		}
	}
	
	/**
	 * 计算平台，广告主对应的佣金比例
	 * @param adx 平台名称
	 * @param advId 广告主id
	 * @return 返回对应的佣金比例
	 */
	public FloatWritable evaluate(String adx, String advId) {
		if (adx == null || adx.trim().length() == 0) {
			return commissionRate;
		}
		
		adx = adx.trim();
		advId = advId.trim();
		Float cr = null;
		
		//获取平台对应的佣金比例，没有该平台默认值为0.1
		Float adxCR = adxCommissionRateMap.get(adx);
		adxCR = (adxCR == null ? 0.1f : adxCR);
		
		//获取广告主对应的佣金比例
		Map<String,Float> advCRMap = advCommissionRateMap.get(adx);
		if (advCRMap != null) {
			cr = advCRMap.get(advId);
		}
		
		cr = (cr == null ? adxCR : cr);
		commissionRate.set(cr.floatValue());
		
		return commissionRate;
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
		String line = null, adx = null, advId = null;
		Float cr = null;
		
		//load adx platform's commission rate config file
		try {
			fs = FileSystem.get(URI.create(adxConfPath), conf);
			in = fs.open(new Path(adxConfPath));
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				//adx,commission rate,adjust price
				if (arr.length >= 2) { //有效值
					adx = arr[0].trim();
					try {
						cr = Float.parseFloat(arr[1].trim());
					} catch (Exception ex) {
						ex.printStackTrace();
						cr = 0.1f;
					}
					
					adxCommissionRateMap.put(adx, cr);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		//load advertiser commission rate config file
		try {
			fs = FileSystem.get(URI.create(advConfPath), conf);
			in = fs.open(new Path(advConfPath));
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				//adx,advId,commission_rate,adjust_price
				if (arr.length >= 3) { //有效值
					adx = arr[0].trim();
					advId = arr[1].trim();
					try {
						cr = Float.parseFloat(arr[2].trim());
					} catch (Exception ex) {
						cr = 0.1f;
					}
					
					Map<String,Float> map = advCommissionRateMap.get(adx);
					if (map == null) {
						map = new ConcurrentHashMap<String,Float>();
					}
					
					map.put(advId, cr);
					advCommissionRateMap.put(adx, map);
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
