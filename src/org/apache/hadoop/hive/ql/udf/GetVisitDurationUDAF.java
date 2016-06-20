package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 计算用户在一次会话中的访问时长,单位为秒
 * 函数签名　get_visit_duration(time)，返回时长 
 * 函数参数: time 各次访问的页面时间 
 * @author jiaqiang
 * 2013.09.27
 */
public class GetVisitDurationUDAF extends UDAF {
	
	public static class StrJoinEvaluator implements UDAFEvaluator {
		
		private static class JoinResult {
			long minTime = 0L; 
			long maxTime = 0L;
			
			public JoinResult() {
			}
		}	
		
		//join
		private JoinResult result;
		
		public StrJoinEvaluator() {
			super();
			init();
		}
		
		@Override
		public void init() {
			result = null;
		}
	
		/**
		 * 遍历记录
		 * @param tiem 时间戳
		 * @return
		 */
		public boolean iterate(long time) {
			if (result == null) {
				result = new JoinResult();
				
				result.minTime = time;
				result.maxTime = time;
				return true;
			}
			
			if (time < result.minTime) {
				result.minTime = time;
			}
			if (time > result.maxTime) {
				result.maxTime = time;
			}
			
			return true;
		}
		
		//处理iterate结果
		public JoinResult terminatePartial() {
			return result;
		}
		
		/**
		 * 合并其它的结果
		 * @param otherResult
		 * @return
		 */
		public boolean merge(JoinResult otherResult) {
			if (otherResult == null) {
				return true;
			}
			
			if (result == null) {
				result = otherResult;
			} else {
				if (otherResult.minTime < result.minTime) {
					result.minTime = otherResult.minTime;
				}
				if (otherResult.maxTime > result.maxTime) {
					result.maxTime = otherResult.maxTime;
				}
			}
			
			
			return true;
	    }
		 		 
		public int terminate() {
			return (int) (result.maxTime - result.minTime);
		}
	}
	
}
