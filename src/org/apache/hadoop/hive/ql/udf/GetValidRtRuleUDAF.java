package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.util.Calendar;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * ��ȡ��Ч��ͷ�͹���
 * ��Ҫ���ݹ������Ч��ȥ�����ڹ���
 * @author jiaqiang
 * 2013.06.07
 */ 
public class GetValidRtRuleUDAF extends UDAF {

	static Logger logger = Logger.getLogger(GetValidRtRuleUDAF.class) ;
	
	//��ͷ�͹�������,�ļ�ֻ���������ֶ�rule_id����Ч����
	static String dspRtRulePath = "/data/stg/dsp_rt_rule/rt_rule.dat";
	
	//RT����(key:rule_id value:��Ч����) 
	static ConcurrentHashMap<String, Integer> ruleMap;
	
	//�����������
	static String runDayId;
	
	//RtRule������
	public static class RtRuleEvaluator implements UDAFEvaluator {
	
		//join���
		JoinResult result;
		
		private static class JoinResult {
			//����id��(�ַ���)������Զ��ŷָ�
			String ruleIds;
			
			//ʱ���
			Long ts = 0L;
		
			public JoinResult() {
			}
		}	
	
		public RtRuleEvaluator() {
			super();
			init(); 
		}
	    
		@Override
		public void init() {
			result = null;
			clear();
			loadRtRuleData();
		} 
		   
		/**
		 * ��������
		 * @param ruleIds ����id������ֵ�Զ��ŷָ�
		 * @param ts ʱ���,��ʽ yyyyMMddHHmmss
		 * @param dayId ��������ID
		 * @return
		 * @throws IOException
		 */
		public boolean iterate(String ruleIds, Long ts, String dayId) throws IOException {
			if (dayId != null && runDayId == null) {
				runDayId = dayId;
			}
			
			if (result == null) {
				result = new JoinResult();
				result.ruleIds = ruleIds;
				result.ts = ts;
				
				return true;
			}
			
			if (ruleIds != null && ts != null) {
				if (ts.compareTo(result.ts) > 0) {
					result.ruleIds = ruleIds;
					result.ts = ts;
				}
			}
			
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
			if (otherResult == null) {
				return true;
			}
			
			if (result == null) { //��
				result = new JoinResult();
			} else {
				if (otherResult.ts.compareTo(result.ts) > 0) {
					result.ruleIds = otherResult.ruleIds;
					result.ts = otherResult.ts;
				}
			}
			
			return true;
		}
		 		 
		//��Ҫ���ݹ�����Ч�ڽ��й��ڹ����ȥ��
		public String terminate() {
			Calendar lastProcessCal = Calendar.getInstance();
			Calendar currCal = Calendar.getInstance();
			currCal.set(Integer.parseInt(runDayId.substring(0,4)), 
					    Integer.parseInt(runDayId.substring(4,6)), 
					    Integer.parseInt(runDayId.substring(6,8)));
			
			StringBuilder buff = new StringBuilder(200);
			
			if (result != null) {
				String[] ruleIdArr = result.ruleIds.split(",");
				String ts = result.ts + "";
				
				for (int i = 0, len=ruleIdArr.length; i < len; ++i) {
					String ruleId = ruleIdArr[i];
					Integer days = ruleMap.get(ruleId);
					
					//��������
					lastProcessCal.set(Integer.parseInt(ts.substring(0, 4)), 
							           Integer.parseInt(ts.substring(4, 6)), 
							           Integer.parseInt(ts.substring(6,8)));
					if (days != null) {
						lastProcessCal.add(Calendar.DAY_OF_MONTH, days);
					}
					
					if (lastProcessCal.compareTo(currCal) >= 0) {
						buff.append((i != len-1) ? ruleId+"," : ruleId) ;	
					}		
				}
			 
				result = null;
			}

			int len = buff.toString().length();
			return len == 0 ? null : buff.toString();
		}

		//��չ�������
		private void clear() {
			if (ruleMap == null) {
				return;
			}
			
			ruleMap.clear();
		}
	
	} //end UrlEvaluator define

	/**
	 * ��hdfs�ж�ȡ��ͷ�͹�������
	 * @return
	 */
	private static boolean loadRtRuleData() {
		if (ruleMap == null) {
			ruleMap = new ConcurrentHashMap<String,Integer>();
		}
		
		boolean succ = true;
		FSDataInputStream in = null;
		FileSystem fs = null;
		Configuration conf = new Configuration();	
		 
		try {
			fs = FileSystem.get(URI.create(dspRtRulePath), conf);
			in = fs.open(new Path(dspRtRulePath));
			
			String line = null, ruleId = null;
			Integer days = 0;
			
			while ((line = in.readLine()) != null) {
				line = line.trim().replace(" ","");
				if (line.length() == 0) { //���Կ���
					continue;
				}
				
				String[] arr = line.split("\u0001");
				if (arr.length == 2) { //��Ч��
					try {
						//����rule_id
						ruleId = arr[0].trim();
						if (ruleId.length() == 0) {
							continue;
						}
						
						//������Ч����
						days = Integer.parseInt(arr[1].trim());
						
						ruleMap.put(ruleId, days);
					} catch (Exception ex) {
						logger.error(ex);
						continue;
					}
				}
			} //end while
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("������ͷ�͹��������ļ�[" + dspRtRulePath + "]�쳣�� " + e.getMessage());
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		return succ;
	} //end read
}
