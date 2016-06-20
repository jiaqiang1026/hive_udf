package org.apache.hadoop.hive.ql.udf;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 返回MD5值
 * @author jiaqiang
 * 2015.11.23
 */
public class MD5UDF extends UDF {
	
	private static MessageDigest md;
	
	static {
		try {
			md = MessageDigest.getInstance("md5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			md = null;
		}
	}
	
	public String evaluate(final Text str) {
		if (str == null) {
			return null;
		}
		
		String s = str.toString().trim();
		if (s.length() == 0) {
			return null;
		}
			
		byte[] md5 = md5(s);
		
		return md5 == null ? null : bytes2hex(md5);
	}
	
	/**
	 * 返回str摘要值
	 * @param str
	 * @return
	 */
	private static byte[] md5(String str) {
	    return md == null ? null : md.digest(str.getBytes());
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
