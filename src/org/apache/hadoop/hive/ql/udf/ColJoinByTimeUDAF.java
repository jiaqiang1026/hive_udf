package org.apache.hadoop.hive.ql.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 列值按时间DESC排序
 * @author jiaqiang
 * 2012.12.20
 */
public class ColJoinByTimeUDAF extends UDAF {
	
	//封装带有时间戳的列值
	static class ColValueWithTime implements Comparable<ColValueWithTime> {
		String cv;    //列值
		Long time;    //时间戳
		
		//按时间从大到小排序
		@Override
		public int compareTo(ColValueWithTime obj) {
			return obj.time.compareTo(time);
		}		
		
		@Override
		public boolean equals(Object o) {
			ColValueWithTime obj = (ColValueWithTime) o;
			
			return obj.cv.equals(this.cv) && obj.time.equals(this.time);
		}
		
		@Override
		public String toString() {
			return "time:" + time + ",value:" + cv;
		}
	}
	
	public static class ColValueWithTimeUDAFEvaluator implements UDAFEvaluator {
		
		private static class JoinResult {
			List<ColValueWithTime> list;
			
			public JoinResult() {
				list = new ArrayList<ColValueWithTime>();				
			}
			
			private void add(ColValueWithTime obj) {
				if (obj != null && !list.contains(obj)) {
					list.add(obj);
				}
			}
			
			private void add(String v, Long time) {
				ColValueWithTime obj = new ColValueWithTime();
				obj.cv = v;
				obj.time = time;
				
				if (!list.contains(obj)) {
					list.add(obj);
				}
			}
		}	
		
		//join结果
		private JoinResult result;
		
		public ColValueWithTimeUDAFEvaluator() {
			super();
			init();
		}
		
		@Override
		public void init() {
			result = null;
		}
	
		/**
		 * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean
		 * @param value 输入列值
		 * @param time 时间戳
		 * @return
		 */
		public boolean iterate(String value, Long time) {
			if (value == null || value.trim().length() == 0 || time == null) {
				return true;
			}
			
			value = value.trim();
			
			if (result == null) {
				result = new JoinResult();
			}
			
			result.add(value,time);
		
			return true;
		}
		
		//处理iterate结果
		public JoinResult terminatePartial() {
			synchronized(this) {
				Collections.sort(result.list);
				
				//保留前20
				for (int i = result.list.size()-1; i > 20; i--) {
					result.list.remove(i);
				}
			}
			
			return result;
		}
		
		/**
		 * 合并
		 * @param otherResult
		 * @return
		 */
		public boolean merge(JoinResult otherResult) {
			if (otherResult == null || otherResult.list.size() == 0) {
				return true;
			}
			
			if (result == null) { //空
				result = new JoinResult();
			} 
			
			synchronized(this) {
				for (int i = 0,size=otherResult.list.size(); i < size; i++) {
					ColValueWithTime obj = otherResult.list.get(i); 
					result.add(obj);	
				}
				
				if (result.list.size() > 100) {
					Collections.sort(result.list);
					
					//保留前20
					for (int i = result.list.size()-1; i > 20; i--) {
						result.list.remove(i);
					}
				}
			}

			return true;
	    }
		 		 
		public String terminate() {
			StringBuilder buff = new StringBuilder(1000);
			if (result != null) {
				Collections.sort(result.list);
				
				for (int i = 0,len=result.list.size(); i < len; i++) {
					ColValueWithTime v = result.list.get(i);
					buff.append((i != len-1) ? v.cv+"," : v.cv) ;
				}
			
				//clear
				result.list.clear();
				result = null;
			}
			
			return buff.toString();
		}
	}
}
