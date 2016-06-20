package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.log4j.Logger;

/**
 * 获取最后一次失败竞价(dim8相同且时间最大)
 * @author jiaqiang
 * 2012.12.03
 */
public class GetLastFailBidPriceUDAF extends UDAF {
	
	private static Logger log = Logger.getLogger(GetLastFailBidPriceUDAF.class);
	
	public static class LastFailBidPriceEvaluator implements UDAFEvaluator {
		
		private static class JoinResult {
			Long time;     //时间
			String price;  //出价

			public JoinResult() {
			}
		}	
		
		//返回值
		private JoinResult result;
		
		public LastFailBidPriceEvaluator() {
			super();
			init();
		}
		
		@Override
		public void init() {
			result = null;
		}
	
		/**
		 * 处理价格与时间
		 * @param price
		 * @param time
		 * @return
		 */
		public boolean iterate(String price, Long time) {
			if (price == null || price.trim().length() == 0) {
				return true;
			}
			if (time == null || time.toString().trim().length() == 0) {
				return true;
			}
			
			if (result == null) {
				result = new JoinResult();
				result.price = price;
				result.time = time;
			} else {
				if (time.compareTo(result.time) > 0) {
					result.price = price;
					result.time = time;
				}
			}
		
			return true;
		}
		
		//
		public JoinResult terminatePartial() {
			return result;
		}
		
		/**
		 * 与另外 一个结果合并
		 * @param otherResult
		 * @return
		 */
		public boolean merge(JoinResult otherResult) {
			if (otherResult == null || otherResult.time == null || otherResult.price == null) {
				return true;
			}
			
			if (result == null) {
				result = otherResult;
				return true;
			} 
			
			synchronized(this) {
				if (otherResult.time.compareTo(result.time) > 0) {
					result.time = otherResult.time;
					result.price = otherResult.price;
				}				
			}

			return true;
	    }
		 		 
		public String terminate() {
			if (result == null) {
				return null;
			}
			
			String s = result.price;
			return (s == null || s.trim().length() == 0) ? null : s;
		}
	}
}
