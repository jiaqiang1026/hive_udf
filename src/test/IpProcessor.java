package test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import util.FileUtil;

/**
 * IP处理器
 * @author jiaqiang
 * 2013.08.02
 */
public class IpProcessor {

	public static void main(String[] args) throws IOException {
		//1.找出中国的ip
		File ipFile = new File("d:\\ip.conf");
		List<String> list = FileUtil.readLines(ipFile);
		List<String> cnIpList = new ArrayList<String>();
		
		StringBuffer buff = new StringBuffer(200);
		
		for (String line : list) {
			if (line.trim().length() > 0 && line.contains("CN")) { //ignore blank line
				String[] arr = line.split("\t");
				String ipSeg = arr[0];
				String code = arr[1].replace("CN", "").replace(";", "");
				int idx = ipSeg.indexOf("-");
				String sIp = ipSeg.substring(0,idx);
				String eIp = ipSeg.substring(idx+1);
				
				Long sIpLong = ip2long(sIp);
				Long eIpLong = ip2long(eIp);
				buff.append(sIpLong).append("\t").append(eIpLong).append("\t").append(code);
				
				cnIpList.add(buff.toString());
				buff.setLength(0);
				
				if (cnIpList.size() >= 1000) {
					if (FileUtil.writeLines(cnIpList, new File("d:\\ip_cn.dat"), "utf-8", true)) {
						cnIpList.clear();
					} else {
						System.out.println("写中国IP数据失败！");
					}
				}
			}
		} //end for
	
		if (cnIpList.size() > 0) {
			if (FileUtil.writeLines(cnIpList, new File("d:\\ip_cn.dat"), "utf-8", true)) {
				cnIpList.clear();
			} else {
				System.out.println("Last time,写中国IP数据失败！");
			}
			
			list.clear();
		}
		
		System.out.println("over.");
	}
	
	
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

}
