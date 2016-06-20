package org.apache.hadoop.hive.ql.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * 将列值join在一起的聚合函数，特点聚合的列有最大数量限制
 * @author jiaqiang
 * 2012.12.05
 */
public class ColJoinWithSizeUDAF extends UDAF {
	
	public static class ColJoinEvaluator implements UDAFEvaluator {
		
		private static class JoinResult {
			List<String> list;   //存储列值集合
			int capacity;        //列值集合最大数量
			
			public JoinResult() {
				list = new ArrayList<String>();				
				capacity = 1;  //default 1
			}
			
			public void add(String v) {
				if (list.size() <= capacity) {
					list.add(v);
				}
			}
		}	
		
		//join结果
		private JoinResult result;
		
		public ColJoinEvaluator() {
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
		 * @param maxSize 最大存储量
		 * @return
		 */
		public boolean iterate(String value, IntWritable maxSize) {
			if (value == null || value.trim().length() == 0 || maxSize == null) {
				return true;
			}
			
			if (result == null) {
				result = new JoinResult();
				
				int c = maxSize.get();
				result.capacity = c < 0 ? 1 : c;
			}
			
			result.add(value);
		
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
			if (otherResult == null || otherResult.list.size() == 0) {
				return true;
			}
			
			if (result == null) { //空
				result = new JoinResult();
			} 
			
			synchronized(this) {
				for (int i = 0,size=otherResult.list.size(); i < size; i++) {
					result.add(otherResult.list.get(i));	
				}
			}

			return true;
	    }
		 		 
		public String terminate() {
			StringBuffer buff = new StringBuffer(1000);
			if (result != null) {
				for (int i = 0,len=result.list.size(); i < len; i++) {
					buff.append((i != len-1) ? result.list.get(i)+"," : result.list.get(i)) ;
				}
			
				//clear
				result.list.clear();
				result = null;
			}
			
			return buff.toString();
		}
	}
}
