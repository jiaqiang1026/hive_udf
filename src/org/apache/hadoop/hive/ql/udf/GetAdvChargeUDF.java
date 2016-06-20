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
 * 暗拍时，计算广告主的一次费用
 * 函数：float get_charge(Integer sourceId,String advId,int dataType,int bidType, int bidFlag,Long maxPrice,Long secondEcpm,Float ctr,Float settlePrice) 
 * 参数1：平台id
 * 参数2：广告主id
 * 参数3：数据类型 0：click数据  1:impression数据
 * 参数4：bidType 出价类型 0:cpc 1:cpm 16:智能投放
 * 参数5：bidFlag 出价标识 0:暗拍  1:明拍
 * 参数6：广告主最高出价
 * 参数7：第二高ecpm2
 * 参数8：预测ctr
 * 参数9：流量成本
 * 返回值: 广主本次计费
 * Modify:加入智能投放(bidtype=16)
 * @author jiaqiang
 * 2014.07.30
 */
public class GetAdvChargeUDF extends UDF {
	
	//adx平台暗拍配置文件
	private final static String confPath = "/user/hive/warehouse/adx_adv_discount/adx_adv_discount.txt";
		
	//key:adx id value:Map<String,Float> [key:advId,value:settlement coefficient(结算系数)]
	private final static Map<Integer,Map<String,Float>> seMap = new ConcurrentHashMap<Integer,Map<String,Float>>();

	static {
		boolean succ = load();
//		System.out.println("加载暗拍配置文件["+confPath + "]" + succ);
//		System.out.println(seMap);
		
		if (!succ) {
			System.out.println("加载暗拍配置文件["+confPath + "]失败");
		}
	}
	
	/**
	 * 计算广告主本次计费
	 * @param sourceId    流量来源，即平台id
	 * @param advId       广告主id
	 * @param dataType    数据类型 0:click数据 1:impression数据
	 * @param bidType     出价类型 0:cpc 1:cpm
	 * @param bidFlag     出价标识 0:暗拍  1:明拍
	 * @param maxPrice    广告主最高出价
	 * @param secondEcpm  次高ecpm
	 * @param ctr         预测ctr
	 * @param settlePrice 流量成本
	 * @return
	 */
	public float evaluate(Integer sourceId, String advId, int dataType, Integer bidType, Integer bidFlag, Long maxPrice,Long secondEcpm, Float ctr, Long settlePrice) {
		if (sourceId == null) { //无平台
			return 0;
		}
				
		if (advId == null || advId.trim().length() == 0) { //无广告主
			return 0;
		}
		
		if (maxPrice == null) { //无出价
			return 0;
		}
		
		if (bidType == null) { //无出价类型
			return maxPrice;
		}
		
		if (bidFlag == null) { //出价标识为空时，按明拍处理
			bidFlag = 1;
		}
		
		if (bidType == 0 && bidFlag == 1) { //固定cpc
			return (dataType == 0 ? (maxPrice == null ? 0 : maxPrice) : 0);
		} else if (bidType == 1 && bidFlag == 1) { //固定cpm
			return (dataType == 1 ? (maxPrice == null ? 0 : maxPrice) : 0);
		} else if (bidType == 0 && bidFlag == 0) { //暗拍cpc
			return (dataType == 0 ? getCPCCharge(sourceId, advId, maxPrice,secondEcpm,ctr,settlePrice) : 0);
		} else if (bidType == 1 && bidFlag == 0) { //暗拍cpm
			return (dataType == 1 ? getCPMCharge(sourceId, advId, maxPrice, secondEcpm,settlePrice) : 0);
		} else if (bidType == 16) { //智能投放,流量成本*1.15
			return (dataType == 1 ? (settlePrice == null ? 0 : settlePrice*1.15f) : 0);
		} else { //其它收费方式
			return 0;
		}
	}

	/**
	 * cpm结算类型 min(max(ecpm_2, settle_price*结算系数)+a，maxPrice)
	 * @param maxPrice    广告主最高出价
	 * @param secondEcpm  最二高ecpm
	 * @param ecpm        预测的ecpm
	 * @param settlePrice 流量成本
	 * @return
	 */
	private Float getCPMCharge(Integer sid, String advId, Long maxPrice, Long secondEcpm, Long settlePrice) {
		if (settlePrice == null) {
			settlePrice = 0L;
		}
		
		//获取结算系数
		Float se = getSE(sid, advId);
		
		//zyz保底ecpm,=流量成本*(1+佣金比例)
		Float zyzEcpm = settlePrice * se;
		
		if (secondEcpm == null) {//无次高ecpm,相当直接比较取 zyz保底ecpm+调节价 与 ecpm的最小值
			secondEcpm = maxPrice;
		}
		
		//找出次高出价与zyz保底ecpm的最大值
		Float max = secondEcpm/1.0f;
		if (max.compareTo(zyzEcpm) < 0) {
			max = zyzEcpm;
		}
		
		//maxprice与 上述max的最小值
		Float min = maxPrice/1.0f;
		if (min.compareTo(max+1) > 0){ 
			min = max+1;
		}
		
		return min;
	}
	
	/** cpc结算类型:min((max(ecpm_2, 流量成本*结算系数)+a)/pctr_1/1000，maxPrice)
	 * 获取点击数据的计费
	 * @param sid             平台
	 * @param advId           广告主id
	 * @param maxBid          广告主最高出价
	 * @param secondEcpm
	 * @param settlePrice
	 * @param ctr
	 * @return
	 */
	private float getCPCCharge(Integer sid, String advId, Long maxPrice, Long secondEcpm, Float ctr, Long settlePrice) {
		if (ctr == null || ctr.compareTo(0.0f) == 0) {
			return maxPrice;
		}
		
		if (settlePrice == null || settlePrice.intValue() == 0) {
			settlePrice = maxPrice;
		}
		
		//获取结算系数
		Float se = getSE(sid, advId);
		
		//zyz保底ecpm,=流量成本*结算系数
		Float zyzEcpm = settlePrice * se;
		
		if (secondEcpm == null) {//无次高出价
			secondEcpm = 0L;
		}
		
		//找出次高ecpm与zyz保底ecpm的最大值
		Float max = secondEcpm/1.0f;
		if (max.compareTo(zyzEcpm) < 0) {
			max = zyzEcpm;
		}
		
		//取最高出价与 上述max的最小值,  再加上调节价，固定值1分
		Float min = maxPrice/1.0f;
		if (min.compareTo((max+1)/ctr/1000) > 0) { 
			min = (max+1)/ctr/1000;
		}
		
		return min;
	}
	
	/**
	 * 从hdfs中加载暗拍配置文件
	 * @return
	 */
	private static synchronized boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		String line = null, advId = null,campId = null, mode = null;
		Integer adxId = null;
		Float account = null;    //结算系数
		
		//load adx platform's commission rate and adjust price config file
		try {
			fs = FileSystem.get(URI.create(confPath), conf);
			in = fs.open(new Path(confPath));
			
			while ((line = in.readLine()) != null) {
				line = line.trim();
				if (line.length() > 0) { //ignore blank line
					String[] arr = line.split(",");
					//adx id,adv id,account
					if (arr.length >= 3) { //有效值
						try {
							adxId = Integer.parseInt(arr[0].trim());
							advId = arr[1].trim();
							account = Float.parseFloat(arr[2].trim());
							
							Map<String,Float> advMap = seMap.get(adxId);
							if (advMap == null) {
								advMap = new ConcurrentHashMap<String,Float>();
							}
							
							advMap.put(advId, account);
							seMap.put(adxId, advMap);
						} catch (Exception ex) {
							ex.printStackTrace();
						}
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
	
	//获取结算系数 
	private Float getSE(Integer sid, String advId) {
		Float se = null; //default value 1.1f
		
		//先查找平台
		Map<String,Float> advMap = seMap.get(sid);
		if (advMap != null) {
			se = advMap.get(advId);
		}
		
		return (se == null ? 1.1f : se);
	}
}
