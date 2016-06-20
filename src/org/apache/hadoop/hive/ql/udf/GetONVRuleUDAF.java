package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * 获取回头客(ole vistor)、新客(new vistor)规则id 
 * 回头客是满足规则url的用户，新客是数据中包含规则id的用户
 * @author jiaqiang
 * 2012.08.15
 */ 
public class GetONVRuleUDAF extends UDAF {

	static Logger logger = Logger.getLogger(GetONVRuleUDAF.class) ;
	
	//dsp提供的url规则数据目录
	static String dspUrlDataDir = "/data/stg/dsp_url_rule/";
	
	//key:level id value:规则url对象集合
	static ConcurrentHashMap<Integer, List<UrlRule>> level2urListMap;

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	//以时间为判断是否重新加载DSP规则url数据,只要不是同一天就加载
	static int dateID;
	
	//url规则
	static class UrlRule {
		String matchedUrl; //匹配url
		int rule;          //匹配规则，1:完全匹配 2:包含
		
		public UrlRule() {
		}
		
		public UrlRule(String matchedUrl,int rule) {
			this.matchedUrl = matchedUrl;
			this.rule = rule;
		}
		
		public String getMatchedUrl() {
			return matchedUrl;
		}
		
		public void setMatchedUrl(String matchedUrl) {
			this.matchedUrl = matchedUrl;
		}
		
		public int getRule() {
			return rule;
		}
		
		public void setRule(int rule) {
			this.rule = rule;
		}
		
		@Override
		public boolean equals(Object obj) {
			UrlRule ur = (UrlRule) obj;
			return ur.matchedUrl.equals(this.matchedUrl) && ur.rule == this.rule;
		}
		
		@Override
		public String toString() {
			return matchedUrl + ":" + rule;
		}
	} //
	
	//url解析器
	public static class UrlEvaluator implements UDAFEvaluator {
	
		//join结果
		JoinResult result;
		
		private static class JoinResult {
			//包含层次id(不重复)
			List<Integer> list;
		
			public JoinResult() {
			}
		}	
	
		public UrlEvaluator() {
			super();
			init(); 
		}
	    
		@Override
		public void init() {
			result = null;
			int currDateID = getCurrentDateID();
			if (currDateID != dateID) {//目前日期 与之前加载的日期不同,重新加载
				clear();
				load();
				dateID = currDateID;
			}
		} 
		   
		/**
		 * 遍历数据
		 * @param url
		 * @param ruleId 如果有值，不考虑url参数
		 * @return
		 * @throws IOException
		 */
		public boolean iterate(String url, Integer ruleId) throws IOException {
			if (result == null) {
				result = new JoinResult();
				result.list = new ArrayList<Integer>();
			}
			
			if (ruleId != null) {
				if (!result.list.contains(ruleId)) {//不包含
					result.list.add(ruleId);
				}
				
				return true;
			}
			
			if (url == null) {
				return true;
			}
			
			url = url.trim();
			if (url.length() == 0) {
				return true;
			}
			
			//获取回头客的层级(规则)id
			List<Integer> levelList = getLevel(url);
			if (levelList != null) {
				for (Integer level : levelList) {
					if (!result.list.contains(level)) {//不包含
						result.list.add(level);
					}
				}
				
				//last clear list
				levelList.clear();
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
				result = new JoinResult();
				result.list = otherResult.list;
			} else {
				for (Integer t : otherResult.list) {
					if (!result.list.contains(t)) {
						result.list.add(t);
					}
				}
			}
			
			return true;
		}
		 		 
		public String terminate() {
			StringBuilder buff = new StringBuilder(200);
			if (result != null) {
				//sort
				Collections.sort(result.list);
				
				for (int i = 0,len=result.list.size(); i < len; i++) {
					buff.append((i != len-1) ? result.list.get(i)+"," : result.list.get(i)) ;
				}
			 
				//clear
				result.list.clear();
				result = null;
			}

			int len = buff.toString().length();
			return len == 0 ? null : buff.toString();
		}
		
		/**
		 * 获取层次id
		 * @param url  
		 * @return
		 */
		private List<Integer> getLevel(String url) {
			if (url == null) {
				return null;
			}
			
			url = url.trim();
			if (url.length() == 0) {
				return null;
			}
			
			List<Integer> levelList = new ArrayList<Integer>();
			for (Integer level : level2urListMap.keySet()) {
				List<UrlRule> urList = level2urListMap.get(level);
				
				for (UrlRule ur : urList) {
					String matchedUrl = ur.getMatchedUrl();
					int rule = ur.getRule();	
					
					if (rule == 1) { //完全匹配
						if (url.equals(matchedUrl)) { //ok
							if (!levelList.contains(level)) {
								levelList.add(level);	
							}
							break;
						}
					} else if (rule == 2) { //包含
						if (url.indexOf(matchedUrl) != -1) { //ok
							if (!levelList.contains(level)) {
								levelList.add(level);	
							}
							break;
						}
					}
				} 
			}
			
			return levelList;
		}

		//清空层级映射数据
		private void clear() {
			if (level2urListMap == null) {
				return;
			}
			  
			for (Integer level : level2urListMap.keySet()) {
				if (level != null) 	{
					List<UrlRule> list = level2urListMap.remove(level);
					if (list != null) {
						list.clear();
					}
					list = null;
				}
			}
			
			level2urListMap.clear();
		}
	
	} //end UrlEvaluator define

	/**
	 * 获取当前日期
	 * @return
	 */
	private static int getCurrentDateID() {
		return Integer.parseInt(sdf.format(new Date()));
	}
	

	/**
	 * 从hdfs中加载dsp给出的url规则数据,找出最大日期下的数据文件
	 * @return true:成功 false:失败
	 */
	private static boolean load() {
		if (level2urListMap == null) {
			level2urListMap = new ConcurrentHashMap<Integer, List<UrlRule>>();
		}
		
		boolean succ = true;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		String maxDateID = null;
		
		try {
			fs = FileSystem.get(URI.create(dspUrlDataDir), conf);
			Path path = new Path(dspUrlDataDir);
		
			//只获取日期目录
			FileStatus[] fsArr = fs.listStatus(path, new PathFilter() {
				@Override
				public boolean accept(Path arg0) {
					String name = arg0.getName();
					return name.matches("^\\d{8,8}$");
				}
			});
			 
			if (fsArr.length == 0) {
				return false;
			}
			
			//找出最大日期目录
			maxDateID = fsArr[0].getPath().getName();
			for (int i = 1,size=fsArr.length; i < size; i++) {
				String dateID = fsArr[i].getPath().getName();
				if (Integer.parseInt(dateID) > Integer.parseInt(maxDateID)) {
					maxDateID = dateID; 
				}
			}
		} catch (IOException ex) {
			logger.error("查找[" + dspUrlDataDir + "]目录下数据最大日期目录失败,[" + ex.getMessage() + "]");
			return false;
		}
		
		////加载数据
		Path dataDir = new Path(dspUrlDataDir + maxDateID);
		try {
			FileStatus[] dataFsArr = fs.listStatus(dataDir);
			for (FileStatus status : dataFsArr) {
				succ = succ && read(status.getPath().toString());
				if (!succ) {
					break;
				}
			}
		} catch (IOException e) {
			logger.error("罗列目录[" + dataDir.toString() + "]子目录时失败,[" + e.getMessage() + "]");
			succ = false;
		} 
	
		return succ;
	}
	
	/**
	 * 读取path文件内容
	 * @param path
	 * @return
	 */
	private static boolean read(String path) {
		if (path == null) {
			return false;
		}
		
		boolean succ = true;
		FSDataInputStream in = null;
		FileSystem fs = null;
		Configuration conf = new Configuration();	
		 
		try {
			fs = FileSystem.get(URI.create(path), conf);
			in = fs.open(new Path(path));
			
			String line = null, matchedUrl = null;
			Integer level = null, rule = null;
			UrlRule urlRule = null;
			
			while ((line = in.readLine()) != null) {
				line = line.trim().replace(" ","");
				if (line.length() == 0) { //忽略空行
					continue;
				}
				
				String[] arr = line.split("\u0001");
				if (arr.length == 3) { //有效值
					//解析出层次id,规则url,操作类型
					try {
						String slevel = arr[0].trim();
						if (slevel.length() == 0) {
							continue;
						}
						level = Integer.parseInt(slevel);
						matchedUrl = arr[1].trim();
						if (matchedUrl.length() == 0) {
							continue;
						}
						String srule = arr[2].trim();
						if (srule.length() == 0) {
							continue;
						}
						rule = Integer.parseInt(srule);
					} catch (Exception ex) {
						logger.error(ex);
						continue;
					}
				
					//无效值
					if (level == null || rule == null || matchedUrl == null || (rule != 1 && rule != 2)) {
						continue;
					}
					
					urlRule = new UrlRule(matchedUrl, rule);
					List<UrlRule> urList = level2urListMap.get(level);
					
					if (urList == null) { //不包含该层
						urList = new ArrayList<UrlRule>();
						urList.add(urlRule);
						
						level2urListMap.put(level, urList);
					} else { //含有该层
						//判断有无
						if (!urList.contains(urlRule)) {//不包括该url规则
				 			urList.add(urlRule);
						}
					}
				}
			} //end while
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("读到文件[" + path + "]异常！ " + e.getMessage());
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		return succ;
	} //end read
}
