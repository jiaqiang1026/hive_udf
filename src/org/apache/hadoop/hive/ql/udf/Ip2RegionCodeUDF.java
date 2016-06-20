package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

/**
 * 将点分ip转换成具休区域UDF
 * @author jiaqiang
 * 2012.06.07
 */
public final class Ip2RegionCodeUDF extends UDF {
	
	//ip区域映射文件路径
	private final static String ipAreaMapPath = "/hive/warehouse/p_ip_dstc_map/ip_cn.dat";
	
	//区域编码文件路径
	private final static String areaCodePath = "/hive/warehouse/p_ip_dstc_map/area_mn.dat";
	
	//区域映射[key:区域编码  value:父级区域编码]
	private static Map<Integer, Integer> areaCodeMap = new ConcurrentHashMap<Integer, Integer>();
		
	//IP&区域集合
	private static List<IpArea> ipAreaList = new ArrayList<IpArea>();
	
	//初使化成功标识
	private static boolean initSucc = false;
	
	//返回的区域编码
	private Text areaCode = new Text();
	
	static {
		boolean succ = load();
		if (succ) {
			initSucc = true;
			
			//排序 
			Collections.sort(ipAreaList);
		}
	}
	
	/**
	 * 返回ip对应的区域编码
	 * @param ip
	 * @return null或区域编码
	 */
	public Text evaluate(final Text ip) {
		return evaluate(ip,false);
	}
	
	/**
	 * 点分ip转成区域ID
	 * @param ip 点分ip地址，如"172.16.2.93"
	 * @param rtnTopLevel 是否返回为顶级编码(省级，直辖市，自治区等一级区域编码)
	 * @return
	 */
	public Text evaluate(final Text ip, Boolean rtnTopLevel) {
		if (!initSucc) { //初使化失败
			return null;
		}
		
		if (ip == null) {
			return null;
		}
		
		final String ipStr = ip.toString().trim();
		if (ipStr.length() == 0) {
			return null;
		}
		
		//点分ip转换成长整型
		Long ipValue = ip2long(ipStr);
		if (ipValue == null) {
			return null;
		}
		
		//查找
		Integer t = find(ipValue);
		if (t != null) {
			areaCode.set(t+"");
		}
		
		if (t == null) {
			return null;
		}
		
		areaCode.set(t+"");
		if (rtnTopLevel == false) {
			return areaCode;
		}
		
		//查找顶级区域编码
		Integer topCode = getTopRegionCode(t);
		if (topCode != null) {
			areaCode.set(topCode+"");
		}
		
		return (topCode == null ? null : areaCode);
	}
	
	/*
	 * 获取参数对应的顶级区域编码 
	 * 如果rc为顶级(父级为86)则直接返回，否则一直向父级找直到找到
	 */
	private Integer getTopRegionCode(Integer rc) {
		Integer parCode = null;
		while ((parCode = areaCodeMap.get(rc)) != null) {
			if (parCode.intValue() == 86) { //is top
				parCode = rc;
				break;
			}
			rc = parCode;
		}
		
		return parCode;
	}
	
	/**
	 * 将点分ip转换成长整型
	 * @param ip
	 * @return
	 */
	public static Long ip2long(String ip) {
		String[] arr = ip.split("\\.");
		int len = arr.length;
		if (len != 4) { //非法表示
			return null;
		}
		
		Long num = 0L;
		try {
			for (int i = 0; i <= 3; i++) {
				num += (Long.parseLong(arr[i]) << ((len-i-1) * 8));
			}
		} catch (Exception ex) {
			num = null;
		}
		
		return num;
	}

	/**
	 * 从hdfs中加载ip区域文件
	 * @return
	 */
	private static synchronized boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			//加载ip对应的区域编码数据
			fs = FileSystem.get(URI.create(ipAreaMapPath), conf);
			in = fs.open(new Path(ipAreaMapPath));
			String line = null;
			IpArea ipArea = null;
			
			while ((line = in.readLine()) != null) {
				//line = line.replace("\u0001", " \u0001");
				String[] arr = line.split("\t");
				if (arr.length == 3) { //有效值
					ipArea = new IpArea();
					try {
						ipArea.setStartIp(Long.parseLong(arr[0].trim()));
						ipArea.setEndIp(Long.parseLong(arr[1].trim()));
						String area = arr[2].trim();
						ipArea.setAreaId(area.length() > 0 ? Integer.parseInt(area) : null);
						
						if (!ipAreaList.contains(ipArea)) { //不包含则加入
							ipAreaList.add(ipArea);
						}
					} catch (Exception ex) {
						continue;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		//加载区域信息数据
		try {
			fs = FileSystem.get(URI.create(areaCodePath), conf);
			in = fs.open(new Path(areaCodePath));
			String line = null;
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				
				try {
					Integer areaCode = Integer.parseInt(arr[0].trim());
					Integer parAreaCode = Integer.parseInt(arr[2].trim());
					areaCodeMap.put(areaCode, parAreaCode);
				} catch (Exception ex) {
					continue;
				}
			}
		} catch (IOException e) {
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		return succ;
	}
	
	/**
	 * 查找ip对应的区域id
	 * @param ip
	 * @return 返回区域ID,无则返回null
	 */
	private Integer find(long ip) {
		Integer areaID = null;
		
		IpArea ipArea = new IpArea();
		ipArea.setStartIp(ip);
		ipArea.setEndIp(ip);
		
		int low = 0;
		int high = ipAreaList.size() - 1;
		
		while (low <= high) {
			int middle = (low + high) / 2;
			IpArea tmp = ipAreaList.get(middle);
			long s = tmp.getStartIp().longValue();
			long e = tmp.getEndIp().longValue();
			
			if (ip >= s && ip <= e) { //期间,找到
				areaID = tmp.getAreaId();
				break;
			} else if (ip < s) {
				high = middle - 1;
			} else {
				low = middle + 1;
			}
		}
		
		return areaID;
	}
	
	/**
	 * IP区域映射类
	 */
	static class IpArea implements Comparable<IpArea> {
		Long startIp;
		Long endIp;
		Integer areaId;
		
		@Override
		public int compareTo(IpArea o) {
			return startIp.compareTo(o.startIp);
		}

		public String toString() {
			return "{'start_ip':" + startIp + ",'end_ip':" + endIp + ",'area_id':" + areaId + "}";
		}
		
		public Long getStartIp() {
			return startIp;
		}

		public void setStartIp(Long startIp) {
			this.startIp = startIp;
		}

		public Long getEndIp() {
			return endIp;
		}

		public void setEndIp(Long endIp) {
			this.endIp = endIp;
		}

		public Integer getAreaId() {
			return areaId;
		}

		public void setAreaId(Integer areaId) {
			this.areaId = areaId;
		}
		
		@Override
		public boolean equals(Object obj) {
			IpArea ia = (IpArea) obj;
			return ia.startIp.equals(this.startIp) && ia.endIp.equals(this.endIp) && ia.areaId.equals(this.areaId);
		}
	}

	public static void main(String[] args) {
//		3757244416
		Long  v = ip2long("60.172.47.67");
		System.out.println(v);
	}
}
