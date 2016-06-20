package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 获取搜索流量来源udaf,根据网站，用户，会话进行分组
 * 函数签名　get_search_source_id(url,ref,flag,time)，返回搜索网站id（flag） 
 * 函数参数: url：用户访问的url; ref:来源url; flag:搜索网站标识，有效值为1-9
 * 返回flag值，有效值范围[1,9]
 * @author jiaqiang
 * 2013.09.05
 */
public class GetSearchTrafficSourceUDAF extends UDAF {
	
	public static class StrJoinEvaluator implements UDAFEvaluator {
		
		private static class JoinResult {
			String url;
			String ref;
			Integer flag;
			Long time;
			
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
		 * @param url 页面url
		 * @param ref 页面来源url
		 * @param flag 搜索网站标识，有效值为1-9
		 * @param tiem 时间戳
		 * @return
		 */
		public boolean iterate(String url, String ref, Integer flag, Long time) {
			if (result == null) {
				result = new JoinResult();
				
				result.url = url;
				result.ref = ref;
				result.flag = flag;
				result.time = time;
				
				return true;
			}
			
			if (time.compareTo(result.time) < 0) {
				result.url = url;
				result.ref = ref;
				result.flag = flag;
				result.time = time;
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
			
			if (result == null || otherResult.time.compareTo(result.time) < 0) {
				result = otherResult;
			} 
			
			return true;
	    }
		 		 
		public int terminate() {
			return result.flag;
		}
	}
	
}
