package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IOUtils;

/**
 * 判断publisher是否是劣质媒体
 * 标准是：满足最小点击数和到达率
 * 函数签名 :int is_bad_publisher(int,int)
 * param 1:点击数
 * param 2:到达数
 * return -1：点击数未到达阈值 0:优质媒体 1:劣质媒体
 * @author jiaqiang
 * 2014-06-24
 */
public final class IsBadPublisherUDF extends UDF {
	
	//rule
	private final static String RULE_PATH = "/user/hive/warehouse/dim_rule_4_badpublisher/dim_rule_4_badpublisher.txt";
	
	//默认最小点击数 
	private static int MIN_CLICK_CNT = 10;
	
	//默认最小到达率
	private static Float MIN_ARRIVATE_RATE= 0.1f;
	
	//初使化成功标识
	private static boolean initSucc = false;
	
	static {
		boolean succ = load();
		if (succ) {
			initSucc = true;
		}
	}
	
	/**
	 * 判断媒体好坏，标准是是否满足最小点击数以及最小到达率
	 * @param clkCnt 点击数
	 * @param arrivalCnt 到达数
	 * @return -1：点击数未到达阈值 0:优质媒体 1:劣质媒体
	 */
	public int evaluate(Integer clkCnt, Integer arrivalCnt) {
		if (!initSucc) { //初使化失败
			return -1;
		}
		
		if (clkCnt < MIN_CLICK_CNT) { //点击数未达到阈值
			return -1;
		}
		
		//最小到达数
		Float minArrivalCnt = clkCnt * MIN_ARRIVATE_RATE;
		if (arrivalCnt*1.0 < minArrivalCnt) { //劣质媒体
			return 1;
		}
		
		return 0;
	}
	
	
	/**
	 * 判断媒体好坏，标准是是否满足最小点击数以及最小到达率
	 * @param clkCnt 点击数
	 * @param arrivalCnt 到达数
	 * @return -1：点击数未到达阈值 0:优质媒体 1:劣质媒体
	 */
	public int evaluate(Long clkCnt, Integer arrivalCnt) {
		if (!initSucc) { //初使化失败
			return -1;
		}
		
		if (clkCnt < MIN_CLICK_CNT) { //点击数未达到阈值
			return -1;
		}
		
		//最小到达数
		Float minArrivalCnt = clkCnt * MIN_ARRIVATE_RATE;
		if (arrivalCnt*1.0 < minArrivalCnt) {
			return 1;
		}
		
		return 0;
	}
	
	
	/**
	 * 判断媒体好坏，标准是是否满足最小点击数以及最小到达率
	 * @param clkCnt 点击数
	 * @param arrivalCnt 到达数
	 * @return -1：点击数未到达阈值 0:优质媒体 1:劣质媒体
	 */
	public int evaluate(Long clkCnt, Long arrivalCnt) {
		if (!initSucc) { //初使化失败
			return -1;
		}
		
		if (clkCnt < MIN_CLICK_CNT) { //点击数未达到阈值
			return -1;
		}
		
		//最小到达数
		Float minArrivalCnt = clkCnt * MIN_ARRIVATE_RATE;
		if (arrivalCnt*1.0 < minArrivalCnt) {
			return 1;
		}
		
		return 0;
	}
	
	/**
	 * 从hdfs中读取规则
	 * @return
	 */
	private static synchronized boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(RULE_PATH), conf);
			in = fs.open(new Path(RULE_PATH));
			String line = null,sid = null;
			Integer adxFlag = 0;
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				if (arr.length >= 2) { //有效值
					try {
						MIN_CLICK_CNT = Integer.parseInt(arr[0]);
						MIN_ARRIVATE_RATE = Float.parseFloat(arr[1]);
					} catch (Exception ex) {
						
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
