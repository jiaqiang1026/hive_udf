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
 * 智云众采取暗拍时的浮动调价值,默认为1,单位为分
 * 函数：int get_adjust_price(string,String) 
 * 参数1：adx平台名称(sid, 如tanx,bes,allyes等),如果无该adx，则返回默认值1
 * 参数2：广告主id
 * 返回值:平台无配置时，返回默认值1;广告主无配置时，返回平台调节价;平台广告主配置时，返回广告主对应调节价
 * @author jiaqiang
 * 2014.02.19
 */
public class GetAdjustPriceUDF extends UDF {
	
	//adx平台对应配置文件
	private final static String adxConfPath = "/user/hive/warehouse/commission/adx_conf.txt";
	
	//adx平台广告主对应配置文件
	private final static String advConfPath = "/user/hive/warehouse/commission/adv_conf.txt";
	
	//key:adx value:adjust price
	private final static Map<String,Integer> adxAdjustPriceMap = new ConcurrentHashMap<String,Integer>();
	
	//key:adx value:Map<String,Integer> [key:advId,value:adjust price]
	private final static Map<String,Map<String,Integer>> advAdjustPriceMap = new ConcurrentHashMap<String,Map<String,Integer>>();
		
	//返回的调节价,默认值为1
	private IntWritable adjustPrice = new IntWritable(1);
	
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
	public IntWritable evaluate(String adx, String advId) {
		if (adx == null || adx.trim().length() == 0) {
			return adjustPrice;
		}
		
		adx = adx.trim();
		advId = advId.trim();
		Integer ap = null;
		
		//获取平台对应的调节价,平台无配置时默认值为1
		Integer adxAP = adxAdjustPriceMap.get(adx);
		adxAP = (adxAP == null ? 1 : adxAP);
		
		//获取广告主对应的调节价
		Map<String,Integer> advAPMap = advAdjustPriceMap.get(adx);
		if (advAPMap != null) {
			ap = advAPMap.get(advId);
		}
		
		ap = (ap == null ? adxAP : ap);
		adjustPrice.set(ap.intValue());
		
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
		String line = null, adx = null, advId = null;
		Integer adjustPrice = null;
		
		//load adx platform's commission rate config file
		try {
			fs = FileSystem.get(URI.create(adxConfPath), conf);
			in = fs.open(new Path(adxConfPath));
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				//adx,commission rate,adjust price
				if (arr.length >= 3) { //有效值
					adx = arr[0].trim();
					try {
						adjustPrice = Integer.parseInt(arr[2].trim());
					} catch (Exception ex) {
						ex.printStackTrace();
						adjustPrice = 1;
					}
					
					adxAdjustPriceMap.put(adx, adjustPrice);
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
				if (arr.length >= 4) { //有效值
					adx = arr[0].trim();
					advId = arr[1].trim();
					try {
						adjustPrice = Integer.parseInt(arr[3].trim());
					} catch (Exception ex) {
						adjustPrice = 1;
					}
					
					Map<String,Integer> map = advAdjustPriceMap.get(adx);
					if (map == null) {
						map = new ConcurrentHashMap<String,Integer>();
					}
					
					map.put(advId, adjustPrice);
					advAdjustPriceMap.put(adx, map);
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
