package org.apache.hadoop.hive.ql.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * ����ֵjoin��һ��ľۺϺ������ص�ۺϵ����������������
 * @author jiaqiang
 * 2012.12.05
 */
public class ColJoinWithSizeUDAF extends UDAF {
	
	public static class ColJoinEvaluator implements UDAFEvaluator {
		
		private static class JoinResult {
			List<String> list;   //�洢��ֵ����
			int capacity;        //��ֵ�����������
			
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
		
		//join���
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
		 * iterate���մ���Ĳ������������ڲ�����ת���䷵������Ϊboolean
		 * @param value ������ֵ
		 * @param maxSize ���洢��
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
		
		//����iterate���
		public JoinResult terminatePartial() {
			return result;
		}
		
		/**
		 * �ϲ�
		 * @param otherResult
		 * @return
		 */
		public boolean merge(JoinResult otherResult) {
			if (otherResult == null || otherResult.list.size() == 0) {
				return true;
			}
			
			if (result == null) { //��
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
