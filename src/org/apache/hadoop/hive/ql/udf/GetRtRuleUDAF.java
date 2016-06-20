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
 * 获取用户回头客规则集(90天范围)
 * 需要根据规则的有效期去除过期规则
 * 对应hive函数名称为get_rt_rule,参数为：规则集(字符串),规则最后计算时间,任务计算日期
 * @author jiaqiang
 * 2013.06.07
 */ 
public class GetRtRuleUDAF extends UDAF {

	//回头客规则数据,文件只包括两个字段rule_id与有效天数
	static String dspRtRulePath = "/data/stg/dsp_rt_rule/a_rt_rule.dat";
	
	//RtRule解析器
	public static class RtRuleEvaluator implements UDAFEvaluator {
		
		//回头客规则
		static class RtRuleResult {
			//规则id集(字符串)，多个以逗号分隔
			String ruleIds;
			
			//时间戳
			Long ts = 0L;

			String runDayId;
			
			public RtRuleResult() {}
		}	
		
		//RT规则(key:rule_id value:有效天数) 
		ConcurrentHashMap<String, Integer> ruleMap;
		
		//结果
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
		 * 遍历数据
		 * @param ruleIds 规则id集，多值以逗号分隔
		 * @param ts 时间戳,格式 yyyyMMddHHmmss
		 * @param dayId 运行日期ID
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
		
		//处理iterate结果
		public RtRuleResult terminatePartial() {
			return result;
		}
		
		/**
		 * 合并
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
		 		 
		//需要根据规则有效期进行过期规则的去除
		public String terminate() {
			StringBuilder buff = new StringBuilder(200);	
		
			if (result != null) {
				Calendar lastProcessCal = Calendar.getInstance();
				Calendar currCal = Calendar.getInstance();
			
				String dayId = result.runDayId;
				String ts = result.ts.toString();
				
				//设置计算日期
				currCal.set(Integer.parseInt(dayId.substring(0,4)), 
						    Integer.parseInt(dayId.substring(4,6)), 
						    Integer.parseInt(dayId.substring(6,8)));
				
				//设置最后计算日期
				lastProcessCal.set(Integer.parseInt(ts.substring(0,4)), 
						           Integer.parseInt(ts.substring(4,6)), 
						           Integer.parseInt(ts.substring(6,8)));
				
				String[] ruleIdArr = result.ruleIds.split(",");
				for (int i = 0, len=ruleIdArr.length; i < len; ++i) {
					String ruleId = ruleIdArr[i];
					
					//取规则有效期
					Integer days = ruleMap.get(ruleId);
				
					//设置最后计算日期
					lastProcessCal.set(Integer.parseInt(ts.substring(0,4)), 
							           Integer.parseInt(ts.substring(4,6)), 
							           Integer.parseInt(ts.substring(6,8)));
					
					if (days != null) {
						//最后计算日期加有效期与任务计算日期比较
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
			
			if (rtRule.charAt(len-1) == ',') { //去除最后一个','
				rtRule = rtRule.substring(0, len-1);
			}
			
			return rtRule;
		}

		/**
		 * 从hdfs中读取回头客规则数据
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
					if (line.length() == 0) { //忽略空行
						continue;
					}
					
					String[] arr = line.split("\u0001");
					if (arr.length == 2) { //有效行
						try {
							//解析rule_id
							ruleId = arr[0].trim();
							if (ruleId.length() == 0) {
								continue;
							}
							
							//解析有效天数
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
