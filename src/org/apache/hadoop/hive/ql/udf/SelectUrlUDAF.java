package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 组装url与host
 * @author jiaqiang
 * 2015.11.04
 */
public class SelectUrlUDAF extends UDAF {
	
	public static class HostUrlEvaluator implements UDAFEvaluator {
		
		private static class JoinResult {
			String host;
			String url;
			
			public JoinResult() {
				
			}
			
			public JoinResult(String host, String url) {
				this.host = host;
				this.url = url;
			}
			
			//根据host检查并替换url值
			public void replace(String otherHost, String otherUrl) {
				if (otherHost != null  
						&& otherHost.trim().length() > 0
						&& !otherHost.equalsIgnoreCase("undefined") 
						&& otherUrl != null && otherUrl.trim().length() > 0) {
					this.host = otherHost;
					this.url = otherUrl;
				}
			}
		}	
		
		//join结果
		private JoinResult result;
		
		public HostUrlEvaluator() {
			super();
			init();
		}
		
		@Override
		public void init() {
			result = null;
		}
		
		public boolean iterate(String url, String host) {
			if (url == null || url.trim().length() == 0) {
				return true;
			}
			
			url = url.trim();
			int idx = url.indexOf("?");
			if (idx != -1) { //去除query
				url = url.substring(0, idx);
			}
			
			if (result == null) {
				result = new JoinResult(host, url);
			} else {
				result.replace(host, url);
			}
		
			return true;
		}
		
		//处理iterate结果
		public JoinResult terminatePartial() {
			return result;
		}
		
		/**
		 * 合并
		 * @param otherResult
		 * @return
		 */
		public boolean merge(JoinResult otherResult) {
			if (otherResult == null) {
				return true;
			}
			
			if (result == null) { //空
				result = new JoinResult(otherResult.host,otherResult.url);
			} else {
				result.replace(otherResult.host,otherResult.url);
			}

			return true;
	    }
		 		 
		public String terminate() {
			StringBuilder buff = new StringBuilder(1000);
			if (result != null) {
				buff.append(result.url + " " + result.host);
			} else {
				buff.append("null null");
			}
			
			return buff.toString();
		}
	}

}
