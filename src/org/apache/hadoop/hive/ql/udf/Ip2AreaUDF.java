package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
public final class Ip2AreaUDF extends UDF {
	
	//ip区域映射文件路径
	private static String ipAreaMapPath = "/hive/warehouse/p_ip_dstc_map/ip_dstc.dat";
	
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
	 * 点分ip转成区域ID
	 * @param ip 点分ip地址，如"172.16.2.93"
	 * @return
	 */
	public Text evaluate(final Text ip) {
		if (!initSucc) { //初使化失败
			return null;
		}
		
		if (ip == null) {
			return null;
		}
		
		String ipStr = ip.toString().trim();
		if (ipStr == null || ipStr.length() == 0) {
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
		
		//查找并返回区域id
		return t == null ? null : areaCode;
	}
	
	/**
	 * 将点分ip转换成长整型
	 * @param ip
	 * @return
	 */
	private Long ip2long(String ip) {
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
	private static boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(ipAreaMapPath), conf);
			in = fs.open(new Path(ipAreaMapPath));
			String line = null;
			IpArea ipArea = null;
			
			while ((line = in.readLine()) != null) {
				line = line.replace("\u0001", " \u0001");
				String[] arr = line.split("\u0001");
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

}
