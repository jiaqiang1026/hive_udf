package org.apache.hadoop.hive.ql.udf;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * device id匹配
 * @author jiaqiang
 * 2016.03.24
 */
public class DeviceIdMatchUDF extends UDF {
	
	private static MessageDigest md4md5;
	private static MessageDigest md4sha1;
	private Text rtn = new Text("0");
	
	static {
		try {
			md4sha1 = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			md4sha1 = null;
		}
		
		try {
			md4md5 = MessageDigest.getInstance("md5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			md4md5 = null;
		}
	}
	
	/**
	 * device id匹配
	 * @param ori  源device_id
	 * @param tar  渠道 device_id
	 * @param channelId 渠道id
	 * @return
	 */
	public Text evaluate(final String ori, final String tar, final String channelId) {
		if (ori == null || tar == null || channelId == null) {
			return rtn;
		}
		
		if (ori.trim().length() == 0 || tar.trim().length() == 0 || channelId.trim().length() == 0) {
			return rtn;
		}
		
		//明文对比
		if (ori.equals(tar)) { 
			rtn.set("1");
			return rtn;
		}
		
		if (channelId.equals("5020")) { //tanx
			if (ori.equalsIgnoreCase(tar)) {
				rtn.set("1");
			}
		} else if (channelId.equals("5050")) { //bes
			//Deivce_id的加密使用MD5加密型Hash算法：
			//MD5(device_id字符串 + 英文逗号 + dsp_id字符串)的32个字节的十六进制表示
			//device_id字符串：全部为大写。优先使用MAC地址，如果没有，使用IMEI号，如果没有，此字段不发送。
			String did = md5(ori.toUpperCase()+",emar-besq");
			if (did.equalsIgnoreCase(tar)) {
				rtn.set("1");
			}
		} else {
			//gdt
			//Android用IMEI md5sum，IOS用IDFA md5sum，其他用MAC地址
			String did = md5(ori);
			if (did.equalsIgnoreCase(tar)) {
				rtn.set("1");
			}
		}
		
		return rtn;
	}
	
	/**
	 * 返回str摘要值
	 * @param str
	 * @return
	 */
	private static String sha1(String str) {
	    return md4sha1 == null ? "" : bytes2hex(md4sha1.digest(str.getBytes()));
	}
	
	public static String md5(final String str) {
		return md4md5 == null ? "" : bytes2hex(str.getBytes());
	}
	
	
	private static String bytes2hex(byte[] bytes) {
		final String HEX = "0123456789abcdef";
		StringBuilder sb = new StringBuilder(bytes.length * 2);
      
		for (byte b : bytes) {
			sb.append(HEX.charAt((b >> 4) & 0x0f));
			sb.append(HEX.charAt(b & 0x0f));
		}

		return sb.toString();
	}
}
