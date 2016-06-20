package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

/**
 * 将点分ip转换成具休区域码UDF,如果该ip为中国，区域码为数字码，如果为外国，则对应国家代码
 * usage: get_region_code(ip),返回区域码
 * @author jiaqiang
 * 2014.02.07
 */
public final class Ip2RegionCodeUDF2 extends UDF {
	
	//ip区域映射文件路径
	private final static String ipAreaMapPath = "/user/hive/warehouse/ip_rep/ip.dat";
		
	//IP&区域集合
	private static ArrayList<Region> ipAreaList = new ArrayList<Region>(250000);
	
	//初使化成功标识
	private static boolean initSucc = false;
	
	//返回的区域编码
	private Text areaCode = new Text();
	
	static {
		boolean succ = load();
		System.out.println("load ip rep["+ipAreaList.size()+"]");
		
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
		String code = find(ipValue);
		if (code != null) {
			areaCode.set(code);
			return areaCode;
		}
	
		return null;
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
			Region ipArea = null;
			
			while ((line = in.readLine()) != null) {
				//line = line.replace("\u0001", " \u0001");
				String[] arr = line.split(",");
				if (arr.length == 3) { //有效值
					ipArea = new Region();
					try {
						ipArea.setStartIp(Long.parseLong(arr[0].trim()));
						ipArea.setEndIp(Long.parseLong(arr[1].trim()));
						ipArea.setCode(arr[2].trim());
						
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
			ipAreaList.clear();
		} finally {
			IOUtils.closeStream(in);
		}
				
		return succ;
	}
	
	/**
	 * 查找ip对应的区域id
	 * @param ip
	 * @return 返回区域码,无则返回null
	 */
	private String find(long ip) {
		String code = null;
		
		Region ipArea = new Region();
		ipArea.setStartIp(ip);
		ipArea.setEndIp(ip);
		
		int low = 0;
		int high = ipAreaList.size() - 1;
		
		while (low <= high) {
			int middle = (low + high) / 2;
			Region tmp = ipAreaList.get(middle);
			long s = tmp.getStartIp().longValue();
			long e = tmp.getEndIp().longValue();
			
			if (ip >= s && ip <= e) { //期间,找到
				code = tmp.getCode();
				break;
			} else if (ip < s) {
				high = middle - 1;
			} else {
				low = middle + 1;
			}
		}
		
		return code;
	}
	
	/**
	 * 区域类
	 */
	static class Region implements Comparable<Region> {
		Long startIp;
		Long endIp;
		String code;
		
		@Override
		public int compareTo(Region o) {
			return startIp.compareTo(o.startIp);
		}

		public String toString() {
			return "{'start_ip':" + startIp + ",'end_ip':" + endIp + ",'region_code':" + code + "}";
		}
		
		public Long getStartIp() {
			return startIp;
		}

		public void setStartIp(Long startIp) {
			this.startIp = startIp;
		}

		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}

		public Long getEndIp() {
			return endIp;
		}

		public void setEndIp(Long endIp) {
			this.endIp = endIp;
		}

		@Override
		public boolean equals(Object obj) {
			Region ia = (Region) obj;
			return ia.startIp.equals(this.startIp) && ia.endIp.equals(this.endIp) && ia.code.equals(this.code);
		}
	}

	public static void main(String[] args) {
//		3757244416
		Long  v = ip2long("60.172.47.67");
		System.out.println(v);
	}
}
