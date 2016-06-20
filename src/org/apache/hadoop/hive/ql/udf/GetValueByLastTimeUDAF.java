package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 根据最大时间获取value值
 * 对应的hive函数签名：String get_value_by_max_time(String value,long time) 参数分别是分组不同的值，时间戳 
 * 函数返回最大时间对应的value值
 * @author jiaqiang
 * 2013.09.09
 */
public class GetValueByLastTimeUDAF extends UDAF {
	
	public static class StrJoinEvaluator implements UDAFEvaluator {
		
		static class ValuePair {
			String value;
			long ts;
			
			public ValuePair() {
			}
			
			public ValuePair(String v, long ts) {
				this.value = v;
				this.ts = ts;
			}
			
			
			public String toString() {
				return value + ":" + ts;
			}
		}
		
		private ValuePair result;
		
		public StrJoinEvaluator() {
			super();
			init();
		}
		
		@Override
		public void init() {
			result = null;
		}
	
		public boolean iterate(String value, long ts) {
			if (result == null) {
				result = new ValuePair(value,ts);
				return true;
			}
			
			//时间戳大，赋值
			if (ts > result.ts) {
				result.value = value;
			}
			
			return true;
		}
		
		public ValuePair terminatePartial() {
			return result;
		}
		
		public boolean merge(ValuePair otherResult) {
			if (otherResult == null) {
				return true;
			}
			
			if (result == null) {
				result = otherResult;
			} else {
				if (otherResult.ts > result.ts) {
					result.value = otherResult.value;
				}
			}
		
			return true;
	    }
		 		 
		public String terminate() {
			return result.value;
		}
	}
}
