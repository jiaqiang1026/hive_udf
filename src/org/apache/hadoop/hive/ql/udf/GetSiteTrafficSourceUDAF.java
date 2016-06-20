package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 获取网站流量来源udaf,根据网站，用户，会话进行分组
 * 函数签名　get_site_traffic_source(url,ref,flag,time)，返回流量来源id 
 * 函数参数: url：用户访问的url; ref:来源url; flag:搜索网站标识，有效值为1-9
 * (1,'搜索引擎'),(2,'外部网站'),(3,'广告投放'),(4,'直接流量') 
 * @author jiaqiang
 * 2013.08.26
 */
public class GetSiteTrafficSourceUDAF extends UDAF {
	
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
			//来自搜索网站
			if (result.flag >= 1 && result.flag <= 9) {
				return 1;
			}
			
			//直接流量
			if (result.ref.trim().length() == 0) {
				return 4;
			}
			
			//其它网站
			return 2;
		}
	}
	
}
