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
 * ��ȡ��ͷ��(ole vistor)���¿�(new vistor)����id 
 * ��ͷ�����������url���û����¿��������а�������id���û�
 * @author jiaqiang
 * 2012.08.15
 */ 
public class GetONVRuleUDAF extends UDAF {

	static Logger logger = Logger.getLogger(GetONVRuleUDAF.class) ;
	
	//dsp�ṩ��url��������Ŀ¼
	static String dspUrlDataDir = "/data/stg/dsp_url_rule/";
	
	//key:level id value:����url���󼯺�
	static ConcurrentHashMap<Integer, List<UrlRule>> level2urListMap;

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	//��ʱ��Ϊ�ж��Ƿ����¼���DSP����url����,ֻҪ����ͬһ��ͼ���
	static int dateID;
	
	//url����
	static class UrlRule {
		String matchedUrl; //ƥ��url
		int rule;          //ƥ�����1:��ȫƥ�� 2:����
		
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
	
	//url������
	public static class UrlEvaluator implements UDAFEvaluator {
	
		//join���
		JoinResult result;
		
		private static class JoinResult {
			//�������id(���ظ�)
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
			if (currDateID != dateID) {//Ŀǰ���� ��֮ǰ���ص����ڲ�ͬ,���¼���
				clear();
				load();
				dateID = currDateID;
			}
		} 
		   
		/**
		 * ��������
		 * @param url
		 * @param ruleId �����ֵ��������url����
		 * @return
		 * @throws IOException
		 */
		public boolean iterate(String url, Integer ruleId) throws IOException {
			if (result == null) {
				result = new JoinResult();
				result.list = new ArrayList<Integer>();
			}
			
			if (ruleId != null) {
				if (!result.list.contains(ruleId)) {//������
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
			
			//��ȡ��ͷ�͵Ĳ㼶(����)id
			List<Integer> levelList = getLevel(url);
			if (levelList != null) {
				for (Integer level : levelList) {
					if (!result.list.contains(level)) {//������
						result.list.add(level);
					}
				}
				
				//last clear list
				levelList.clear();
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
		 * ��ȡ���id
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
					
					if (rule == 1) { //��ȫƥ��
						if (url.equals(matchedUrl)) { //ok
							if (!levelList.contains(level)) {
								levelList.add(level);	
							}
							break;
						}
					} else if (rule == 2) { //����
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

		//��ղ㼶ӳ������
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
	 * ��ȡ��ǰ����
	 * @return
	 */
	private static int getCurrentDateID() {
		return Integer.parseInt(sdf.format(new Date()));
	}
	

	/**
	 * ��hdfs�м���dsp������url��������,�ҳ���������µ������ļ�
	 * @return true:�ɹ� false:ʧ��
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
		
			//ֻ��ȡ����Ŀ¼
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
			
			//�ҳ��������Ŀ¼
			maxDateID = fsArr[0].getPath().getName();
			for (int i = 1,size=fsArr.length; i < size; i++) {
				String dateID = fsArr[i].getPath().getName();
				if (Integer.parseInt(dateID) > Integer.parseInt(maxDateID)) {
					maxDateID = dateID; 
				}
			}
		} catch (IOException ex) {
			logger.error("����[" + dspUrlDataDir + "]Ŀ¼�������������Ŀ¼ʧ��,[" + ex.getMessage() + "]");
			return false;
		}
		
		////��������
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
			logger.error("����Ŀ¼[" + dataDir.toString() + "]��Ŀ¼ʱʧ��,[" + e.getMessage() + "]");
			succ = false;
		} 
	
		return succ;
	}
	
	/**
	 * ��ȡpath�ļ�����
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
				if (line.length() == 0) { //���Կ���
					continue;
				}
				
				String[] arr = line.split("\u0001");
				if (arr.length == 3) { //��Чֵ
					//���������id,����url,��������
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
				
					//��Чֵ
					if (level == null || rule == null || matchedUrl == null || (rule != 1 && rule != 2)) {
						continue;
					}
					
					urlRule = new UrlRule(matchedUrl, rule);
					List<UrlRule> urList = level2urListMap.get(level);
					
					if (urList == null) { //�������ò�
						urList = new ArrayList<UrlRule>();
						urList.add(urlRule);
						
						level2urListMap.put(level, urList);
					} else { //���иò�
						//�ж�����
						if (!urList.contains(urlRule)) {//��������url����
				 			urList.add(urlRule);
						}
					}
				}
			} //end while
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("�����ļ�[" + path + "]�쳣�� " + e.getMessage());
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		return succ;
	} //end read
}
