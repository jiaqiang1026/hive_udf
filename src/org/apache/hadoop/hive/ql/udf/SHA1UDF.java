package org.apache.hadoop.hive.ql.udf;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 返回sha-1值
 * @author jiaqiang
 * 2015.12.23
 */
public class SHA1UDF extends UDF {
	
	private static MessageDigest md;
	
	static {
		try {
			md = MessageDigest.getInstance("SHA-1");
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
			
		byte[] sha1 = sha1(s);
		
		return sha1 == null ? null : bytes2hex(sha1);
	}
	
	/**
	 * 返回str摘要值
	 * @param str
	 * @return
	 */
	private static byte[] sha1(String str) {
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
