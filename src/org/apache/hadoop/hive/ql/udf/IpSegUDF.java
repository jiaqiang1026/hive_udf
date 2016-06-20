package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * ip分段,如127.0.0.1  ip_seg(ip,"A")--->127
 * @author jiaqiang
 * 2012.10.08
 */
public class IpSegUDF extends UDF {
	
	private Text rtn = new Text();
	
	/**
	 * 取第几段ip
	 * @param ip 源串
	 * @return
	 */
	public Text evaluate(String ip, String seg) {
		if (ip == null) {
			return null;
		}
		
		String str = ip.toString().trim();
		if (str.length() == 0) {
			return null;
		}
		
		try {
			String[] arr = str.split("\\.");
			String t = "";
			if (seg.equalsIgnoreCase("A")) {
				t += arr[0];
			} else if (seg.equalsIgnoreCase("B")) {
				t +=  arr[0] + "."+arr[1];
			}  else if (seg.equalsIgnoreCase("C")) {
				t += arr[0] + "."+arr[1]+"."+arr[2];
			} else if (seg.equalsIgnoreCase("D")) {
				t += arr[0]+"."+arr[1]+"."+arr[2];
			} else {
				t += arr[0]+"."+arr[1]+"."+arr[2]+"."+arr[3];
			}
			
			rtn.set(t);
		} catch (Exception ex) {
			return null;
		}
		
		return rtn;
	}
	
	public static void main(String[] args) {
		String ip = "60.28.163.239";
		//System.out.println(ip.split("[.]").length);
		IpSegUDF u = new IpSegUDF();
		System.out.println(u.evaluate(ip,"C"));
		
	}
}
