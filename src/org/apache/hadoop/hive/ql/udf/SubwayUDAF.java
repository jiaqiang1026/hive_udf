package org.apache.hadoop.hive.ql.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 站点信息汇总函数
 * 计算出用户每次乘走地铁的信息
 * @author jiaqiang
 * 2016.06.18
 */
public class SubwayUDAF extends UDAF {
	
	//赤道半径(单位m)
	private static final  double EARTH_RADIUS = 6378137;                                                                       
	    
    /**                                                                                                                                     
     * 转化为弧度(rad)                                                                                                                      
     * */                                                                                                                                   
    private static double rad(double d) {                                                                                                                                       
       return d * Math.PI / 180.0;                                                                                                          
    }                                                                                                                                       
	                                                                                                                                            
    /**                                                                                                                                     
     * 基于余弦定理求两经纬度距离                                                                                                           
     * @param lon1 第一点的经度     
     * @param lat1 第一点的纬度     
     * @param lon2 第二点的经度 
     * @param lat3 第二点的纬度  
     * 
     * @return 返回的距离，单位km                                                                                                     
    */                                                                                                                                   
    public static double distance(double lon1, double lat1,double lon2, double lat2) {                                        
        double radLat1 = rad(lat1);                                                                                                         
        double radLat2 = rad(lat2);                                                                                                         
                                                                                                                                            
        double radLon1 = rad(lon1);                                                                                                         
        double radLon2 = rad(lon2);                                                                                                         
                                                                                                                                            
        if (radLat1 < 0)                                                                                                                    
            radLat1 = Math.PI / 2 + Math.abs(radLat1);// south                                                                              
        if (radLat1 > 0)                                                                                                                    
            radLat1 = Math.PI / 2 - Math.abs(radLat1);// north                                                                              
        if (radLon1 < 0)                                                                                                                    
            radLon1 = Math.PI * 2 - Math.abs(radLon1);// west                                                                               
        if (radLat2 < 0)                                                                                                                    
            radLat2 = Math.PI / 2 + Math.abs(radLat2);// south                                                                              
        if (radLat2 > 0)                                                                                                                    
            radLat2 = Math.PI / 2 - Math.abs(radLat2);// north                                                                              
        if (radLon2 < 0)                                                                                                                    
            radLon2 = Math.PI * 2 - Math.abs(radLon2);// west                                                                               
        double x1 = EARTH_RADIUS * Math.cos(radLon1) * Math.sin(radLat1);                                                                   
        double y1 = EARTH_RADIUS * Math.sin(radLon1) * Math.sin(radLat1);                                                                   
        double z1 = EARTH_RADIUS * Math.cos(radLat1);                                                                                       
                                                                                                                                            
        double x2 = EARTH_RADIUS * Math.cos(radLon2) * Math.sin(radLat2);                                                                   
        double y2 = EARTH_RADIUS * Math.sin(radLon2) * Math.sin(radLat2);                                                                   
        double z2 = EARTH_RADIUS * Math.cos(radLat2);                                                                                       
                                                                                                                                            
        double d = Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2)+ (z1 - z2) * (z1 - z2));                                         
        //余弦定理求夹角                                                                                                                    
        double theta = Math.acos((EARTH_RADIUS * EARTH_RADIUS + EARTH_RADIUS * EARTH_RADIUS - d * d) / (2 * EARTH_RADIUS * EARTH_RADIUS));  
        double dist = theta * EARTH_RADIUS / 1000;                                                                                                 
     
        return dist;
    }

	public static class RecordJoinEvaluator implements UDAFEvaluator {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		
		//封装单条记录
		class Record implements Comparable<Record> {
			String uid;      //用户id
			String station;  //站号（可能包含中间站点以及换乘站)
			String time;     //时间，格式 yyyy-MM-dd HH:mm
			long ts = 0;     //时间戳整型值
			double lon;      //经度
			double lat;      //纬度
			
			String line;     //几号线，如L1,L2,L10
			int statNumber;  //站号,如8，9，10
			
			//key:line value:statNumber
			Map<String,Integer> transfers = new HashMap<String,Integer>(); //对应的换乘站
			
			public Record(String uid, String station, String time, double lon, double lat) {
				super();
				this.uid = uid;
				
				this.station = station;
				
				//站号处理
				String[] arr = station.split("[|]");
								
				String s = "";
				//换乘站信息提取
				for (int i = 0; i < arr.length; i++) {
					s = arr[i];
					int idx = s.indexOf(".");
					if (idx != -1) {
						s = s.substring(0,idx);
					}
					
					idx = s.indexOf("_");
					if (idx != -1) {
						if (i == 0) {
							line = s.substring(0,idx);
							statNumber = Integer.parseInt(s.substring(idx+1));
						} else {
							transfers.put(s.substring(0,idx), Integer.parseInt(s.substring(idx+1)));
						}
					}
				}
			
				this.time = time;
				try {
					this.ts = sdf.parse(this.time).getTime();
				} catch (ParseException e) {
				}
				
				this.lon = lon;
				this.lat = lat;
			}

			/**
			 * 判断是否同次乘走
			 * 标准：时间间隔<15m,距离<5km,站数间隔<3
			 * @param r
			 * @return
			 */
			public boolean isSameTake(Record r) {
				//时间间隔
				int timeInterval = (int) Math.abs((this.ts-r.ts)/60000);
				
				//距离
				double distance = distance(this.lon,this.lat,r.lon,r.lat);
				
				//站点间隔
				int statInterval = statInterval(r);
				
				return timeInterval < 15 && distance < 5 && statInterval < 3;
			}
			
			//站点差
			public int statInterval(Record r) {
				int i = 0;
				if (r.line.equalsIgnoreCase(this.line)) { //线路相同
					i = Math.abs(r.statNumber-this.statNumber);
				} else { //线路不同
					if (this.transfers.containsKey(r.line)) { 
						i = Math.abs(this.transfers.get(r.line)-r.statNumber);
					}
				}
				
				return i;
			}
			
			@Override
			public int compareTo(Record r) {
				return (int) (this.ts - r.ts);
			}
		}
		
		class JoinResult {
			List<Record> recordList;
			
			public JoinResult() {
				recordList = new ArrayList<Record>(36);				
			}
			
			public JoinResult(List<Record> recordList) {
				if (recordList != null) {
					this.recordList = recordList;
				}
			}
			
			//添加记录
			private void add(Record r) {
				if (recordList == null) {
					recordList = new ArrayList<Record>(36);
					recordList.add(r);
				} else { //存在uid
					boolean flag = false;
					
					//如果站点存在，且时间间隔为3分钟内，不添加
					for (Record curr : recordList) {
						if (r.station.equalsIgnoreCase(curr.station)) { //站点相同,判断时间间隔
							if (Math.abs(r.ts-curr.ts) <= 3) { //重复记录,不加入
								flag = true;
								break;
							}
						}
					}
					if (!flag) {
						recordList.add(r);
					}
				}
			}
		}	
		
		//join结果
		private JoinResult result;
		
		public RecordJoinEvaluator() {
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
		public boolean iterate(String uid, String station, String time, double lon, double lat) {
			if (uid == null || uid.trim().length() == 0) {
				return true;
			}
			
			uid = uid.trim();
			Record r = new Record(uid,station,time,lon,lat);
			
			if (result == null) {
				result = new JoinResult();
			} else {
				result.add(r);
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
			if (otherResult == null || otherResult.recordList.size() == 0) {
				return true;
			}
			
			if (result == null) { //空
				result = new JoinResult(otherResult.recordList);
			} 
			
			//合并
			synchronized(this) {
				for (Record r : otherResult.recordList) {
					result.add(r);		
				}
			}

			return true;
	    }
		 		 
		public String terminate() {
			StringBuilder buff = new StringBuilder(1000);
			
			if (result != null) {
				//排序,按时间从小到大
				Collections.sort(result.recordList);
			
				List<Record> resultList = result.recordList;
			
				buff.append("[");
				//首站
				Record r = resultList.get(0);
				buff.append("{'");
				
				//站数，换乘数，
				int stations = 1, transfers = 0;
				String startTime = r.time, endTime = r.time;
				
				for (int i = 1; i < resultList.size()-1; i++) {
					Record next = resultList.get(i);
					
				}
				  
				//clear
				result.recordList.clear();
				result = null;
			}
			
			return buff.toString();
		}
	}
	
}
