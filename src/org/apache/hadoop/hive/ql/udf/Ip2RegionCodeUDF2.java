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
 * �����ipת���ɾ���������UDF,�����ipΪ�й���������Ϊ�����룬���Ϊ��������Ӧ���Ҵ���
 * usage: get_region_code(ip),����������
 * @author jiaqiang
 * 2014.02.07
 */
public final class Ip2RegionCodeUDF2 extends UDF {
	
	//ip����ӳ���ļ�·��
	private final static String ipAreaMapPath = "/user/hive/warehouse/ip_rep/ip.dat";
		
	//IP&���򼯺�
	private static ArrayList<Region> ipAreaList = new ArrayList<Region>(250000);
	
	//��ʹ���ɹ���ʶ
	private static boolean initSucc = false;
	
	//���ص��������
	private Text areaCode = new Text();
	
	static {
		boolean succ = load();
		System.out.println("load ip rep["+ipAreaList.size()+"]");
		
		if (succ) {
			initSucc = true;
			
			//���� 
			Collections.sort(ipAreaList);
		}
		
	}
	
	/**
	 * ���ipת������ID
	 * @param ip ���ip��ַ����"172.16.2.93"
	 * @return
	 */
	public Text evaluate(final Text ip) {
		if (!initSucc) { //��ʹ��ʧ��
			return null;
		}
		
		if (ip == null) {
			return null;
		}
		
		final String ipStr = ip.toString().trim();
		if (ipStr.length() == 0) {
			return null;
		}
		
		//���ipת���ɳ�����
		Long ipValue = ip2long(ipStr);
		if (ipValue == null) {
			return null;
		}
		
		//����
		String code = find(ipValue);
		if (code != null) {
			areaCode.set(code);
			return areaCode;
		}
	
		return null;
	}
	
	/**
	 * �����ipת���ɳ�����
	 * @param ip
	 * @return
	 */
	public static Long ip2long(String ip) {
		String[] arr = ip.split("\\.");
		int len = arr.length;
		if (len != 4) { //�Ƿ���ʾ
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
	 * ��hdfs�м���ip�����ļ�
	 * @return
	 */
	private static synchronized boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			//����ip��Ӧ�������������
			fs = FileSystem.get(URI.create(ipAreaMapPath), conf);
			in = fs.open(new Path(ipAreaMapPath));
			String line = null;
			Region ipArea = null;
			
			while ((line = in.readLine()) != null) {
				//line = line.replace("\u0001", " \u0001");
				String[] arr = line.split(",");
				if (arr.length == 3) { //��Чֵ
					ipArea = new Region();
					try {
						ipArea.setStartIp(Long.parseLong(arr[0].trim()));
						ipArea.setEndIp(Long.parseLong(arr[1].trim()));
						ipArea.setCode(arr[2].trim());
						
						if (!ipAreaList.contains(ipArea)) { //�����������
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
	 * ����ip��Ӧ������id
	 * @param ip
	 * @return ����������,���򷵻�null
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
			
			if (ip >= s && ip <= e) { //�ڼ�,�ҵ�
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
	 * ������
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
