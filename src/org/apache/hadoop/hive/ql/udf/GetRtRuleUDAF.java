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

/**
 * ��ȡ�û���ͷ�͹���(90�췶Χ)
 * ��Ҫ���ݹ������Ч��ȥ�����ڹ���
 * ��Ӧhive��������Ϊget_rt_rule,����Ϊ������(�ַ���),����������ʱ��,�����������
 * @author jiaqiang
 * 2013.06.07
 */ 
public class GetRtRuleUDAF extends UDAF {

	//��ͷ�͹�������,�ļ�ֻ���������ֶ�rule_id����Ч����
	static String dspRtRulePath = "/data/stg/dsp_rt_rule/a_rt_rule.dat";
	
	//RtRule������
	public static class RtRuleEvaluator implements UDAFEvaluator {
		
		//��ͷ�͹���
		static class RtRuleResult {
			//����id��(�ַ���)������Զ��ŷָ�
			String ruleIds;
			
			//ʱ���
			Long ts = 0L;

			String runDayId;
			
			public RtRuleResult() {}
		}	
		
		//RT����(key:rule_id value:��Ч����) 
		ConcurrentHashMap<String, Integer> ruleMap;
		
		//���
		RtRuleResult result;
	
		static boolean initFlag = false;
		
		public RtRuleEvaluator() {
			super();
			init(); 
		}
	    
		@Override
		public void init() {
			result = null;
			if (initFlag == false) {
				loadRtRuleData();
				initFlag = true;
			}
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
			if (result == null) {
				result = new RtRuleResult();
				result.ruleIds = ruleIds;
				result.ts = ts;
				result.runDayId = dayId;
				
				return true;
			}
			
			if (ruleIds != null && ts != null && dayId != null) {
				if (ts.compareTo(result.ts) > 0) {
					result.ruleIds = ruleIds;
					result.ts = ts;
					result.runDayId = dayId;
				}
			}
			
			return true;
		}
		
		//����iterate���
		public RtRuleResult terminatePartial() {
			return result;
		}
		
		/**
		 * �ϲ�
		 * @param otherResult
		 * @return
		 */
		public boolean merge(RtRuleResult otherResult) {
			if (otherResult == null) {
				return true;
			}
			
			if (result == null || otherResult.ts.compareTo(result.ts) > 0) {
				result = otherResult;
			} 
			
			return true;
		}
		 		 
		//��Ҫ���ݹ�����Ч�ڽ��й��ڹ����ȥ��
		public String terminate() {
			StringBuilder buff = new StringBuilder(200);	
		
			if (result != null) {
				Calendar lastProcessCal = Calendar.getInstance();
				Calendar currCal = Calendar.getInstance();
			
				String dayId = result.runDayId;
				String ts = result.ts.toString();
				
				//���ü�������
				currCal.set(Integer.parseInt(dayId.substring(0,4)), 
						    Integer.parseInt(dayId.substring(4,6)), 
						    Integer.parseInt(dayId.substring(6,8)));
				
				//��������������
				lastProcessCal.set(Integer.parseInt(ts.substring(0,4)), 
						           Integer.parseInt(ts.substring(4,6)), 
						           Integer.parseInt(ts.substring(6,8)));
				
				String[] ruleIdArr = result.ruleIds.split(",");
				for (int i = 0, len=ruleIdArr.length; i < len; ++i) {
					String ruleId = ruleIdArr[i];
					
					//ȡ������Ч��
					Integer days = ruleMap.get(ruleId);
				
					//��������������
					lastProcessCal.set(Integer.parseInt(ts.substring(0,4)), 
							           Integer.parseInt(ts.substring(4,6)), 
							           Integer.parseInt(ts.substring(6,8)));
					
					if (days != null) {
						//���������ڼ���Ч��������������ڱȽ�
						lastProcessCal.add(Calendar.DAY_OF_MONTH, days-1);
						if (lastProcessCal.compareTo(currCal) >= 0) {
							buff.append((i != len-1) ? ruleId+"," : ruleId) ;	
						}		
					}
				}
				
				result = null;
			}

			String rtRule = buff.toString();
			int len = rtRule.length();
			if (len == 0) {
				return null;
			}
			
			if (rtRule.charAt(len-1) == ',') { //ȥ�����һ��','
				rtRule = rtRule.substring(0, len-1);
			}
			
			return rtRule;
		}

		/**
		 * ��hdfs�ж�ȡ��ͷ�͹�������
		 * @return
		 */
		private boolean loadRtRuleData() {
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
							ex.printStackTrace();
							continue;
						}
					}
				} //end while
			} catch (IOException e) {
				e.printStackTrace();
				succ = false;
			} finally {
				IOUtils.closeStream(in);
			}
			
			return succ;
		} //end read
		
	} //end RtRuleEvaluator define
}
