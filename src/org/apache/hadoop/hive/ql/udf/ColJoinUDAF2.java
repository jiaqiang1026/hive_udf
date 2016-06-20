package org.apache.hadoop.hive.ql.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 将分组后的列以特定分隔符拼接起来
 * 函数：string col_join(string,string,boolean)
 * 参数1：列名，参数2：分隔串 参数3：是否去重
 * 返回分组后的列拼接串,以指定的分隔符分隔
 * @author jiaqiang
 * 2014.02.08
 */
public class ColJoinUDAF2 extends UDAF {
	
	public static class StrJoinEvaluator implements UDAFEvaluator {
		
		private static class JoinResult {
			List<String> list;
			String delimit;   //分隔符
			boolean distinct; //是否去重
			
			public JoinResult() {
				this.list = new ArrayList<String>();				
				this.delimit = ",";
				this.distinct = false;
			}
			
			public JoinResult(String v,String delimit,boolean distinct) {
				this.list = new ArrayList<String>();				
				this.delimit = delimit;
				this.distinct = distinct;
				
				list.add(v);
			}
			
			public JoinResult(String delimit, boolean distinct) {
				this.list = new ArrayList<String>();				
				this.delimit = delimit;
				this.distinct = distinct;
			}
			
			private boolean add(String v) {
				if (list.size() <= 1000) {
					if (distinct) { //去重
						if (!list.contains(v)) {
							list.add(v);
						}
					} else {
						return list.add(v);	
					}
				}
				
				return false;
			}
		}	
		
		//join结果
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
		 * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean
		 * @param value 输入列值
		 * @param delimit 分隔符
		 * @return
		 */
		public boolean iterate(String value, String delimit, boolean distinct) {
			if (value == null || value.trim().length() == 0) {
				return true;
			}
			
			value = value.trim();
			
			if (result == null) {
				result = new JoinResult(value,delimit,distinct);
			} else {
				result.add(value);
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
			if (otherResult == null || otherResult.list.size() == 0) {
				return true;
			}
			
			if (result == null) { //空
				result = new JoinResult(otherResult.delimit,otherResult.distinct);
			} 
			
			synchronized(this) {
				for (int i = 0,size=otherResult.list.size(); i < size; i++) {
					result.add(otherResult.list.get(i));	
				}
			}

			return true;
	    }
		 		 
		public String terminate() {
			StringBuilder buff = new StringBuilder(1000);
			if (result != null) {
				for (int i = 0,len=result.list.size(); i < len; i++) {
					buff.append((i != len-1) ? result.list.get(i)+result.delimit : result.list.get(i)) ;
				}
			
				//clear
				result.list.clear();
				result = null;
			}
			
			return buff.toString();
		}
	}
	
}
