package org.apache.hadoop.hive.ql.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 获取网站的退出页面
 * 对应的hive函数签名：String get_exit_pages(String sid,String url,long time) 参数分别是网站id,页面url(md5),页面访问时间
 * 函数返回退出页面字符串，多个值以逗号分隔
 * @author jiaqiang
 * 2013.07.23
 */
public class GetExitPageUDAF extends UDAF {
	
	public static class StrJoinEvaluator implements UDAFEvaluator {
		
		static class ExitPage {
			String sid;
			long firstTime;
			String url;
			long time;
			
			public ExitPage() {
			}
			
			public String toString() {
				return sid + ":" + firstTime + ":" + url + ":" + time;
			}
		}
		
		private static class JoinResult {
			List<ExitPage> pageList;
			
			public JoinResult() {
				pageList = new ArrayList<ExitPage>();				
			}
			
			private void add(String sid, String url, long time) {
				ExitPage ep = null;
				if (pageList.size() == 0) { // 暂无元素
					ep = new ExitPage();
					ep.sid = sid;
					ep.url = url;
					ep.firstTime = time;
					ep.time = time;
					pageList.add(ep);
				} else {
					// 取最后一个
					ep = pageList.get(pageList.size() - 1);
					if (sid.equals(ep.sid)) { // 参数sid与最后元素的网站id相同
						if (time > ep.time) { // 更新页面，取最大时间对应的页面url作为退出页面
							ep.url = url;
							ep.time = time;
						} else {
							ep.firstTime = time;
						}
					} else { // 网站id不同,直接添加
						ep = new ExitPage();
						ep.sid = sid;
						ep.url = url;
						ep.firstTime = time;
						ep.time = time;
						pageList.add(ep);
					}
				}
			}
			
			//按网站出现时间从小到大排序
			public void sort() {
				if (pageList != null && pageList.size() > 0) {
					Collections.sort(pageList, new Comparator() {
						@Override
						public int compare(Object o1, Object o2) {
							ExitPage ep1 = (ExitPage) o1;
							ExitPage ep2 = (ExitPage) o2;
							return (int) (ep1.firstTime - ep2.firstTime);
						}
					});
				}
			}
			
			public String toString() {
				return pageList.toString();
			}
		} //end JoinResult
		
		private JoinResult result;
		
		public StrJoinEvaluator() {
			super();
			init();
		}
		
		@Override
		public void init() {
			result = null;
		}
	
		public boolean iterate(String sid, String url, long time) {
			if (result == null) {
				result = new JoinResult();
			}
			
			result.add(sid,url,time);
		
			return true;
		}
		
		public JoinResult terminatePartial() {
			result.sort();
			return result;
		}
		
		public boolean merge(JoinResult otherResult) {
			if (otherResult == null) {
				return true;
			}
			
			if (result == null) {
				result = otherResult;
			} else {
				result.pageList.addAll(otherResult.pageList);
				result.sort();

				for (int i = 0, size = result.pageList.size(); i < size-1; i++) {
					ExitPage currEP = result.pageList.get(i);
					ExitPage nextEP = result.pageList.get(i + 1);
					if (currEP.sid.equals(nextEP.sid)) {
						nextEP.firstTime = currEP.firstTime;
						result.pageList.set(i, null);
					}
				}
			}
		
			return true;
	    }
		 		 
		public String terminate() {
			if (result == null || result.pageList.size() == 0) {
				return null;
			}
			
			StringBuffer buff = new StringBuffer(200);
			for (int i = 0, size = result.pageList.size(); i < size; ++i) {
				ExitPage ep = result.pageList.get(i);
				if (ep != null) {
					buff.append((i != size-1) ? ep.url + "," : ep.url);
				}
			}

			return buff.toString();
		}
	}
}
