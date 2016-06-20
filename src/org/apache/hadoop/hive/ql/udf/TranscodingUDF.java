package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 字符串转码函数
 * @author jiaqiang
 * 2013.04.09
 */
public class TranscodingUDF extends UDF {

	private Text result = new Text();
	
	private StringBuffer buff = new StringBuffer();  
	
	/**
	 * 将原先encodeCharset字符集的字符串转成以targetCharset的字符集
	 * @param str
	 *           处理的字符串
	 * @param charset
	 *           字符串原先的字符集
	 * @param targetCharset
	 *           要转换的最终字符集
	 * @return
	 */
	public Text evaluate(String str, String charset, String targetCharset) {
		if (str == null) {
			return null;
		}
		
		System.out.println("search_word:" + str);
		
		try {
			result.set(new String(str.getBytes(charset), targetCharset));
		} catch (Exception e) {
			return null;
		}
		
		return result;
	}
	
	/**
	 * 将原先encodeCharset字符集的字符串转成以targetCharset的字符集
	 * @param str
	 *           处理的字符串
	 * @param charset
	 *           字符串原先的字符集
	 * @param targetCharset
	 *           要转换的最终字符集
	 * @return
	 */
	public Text evaluate(String str, String charset) {
		return evaluate(str, charset, "utf-8");
	}
	
	/**
	 * 默认gbk编码，默认转udf-8编码
	 * @param str
	 *           处理的字符串
	 */
	public Text evaluate(String str) {
		if (str == null) {
			return null;
		}
		
		byte[] utf8 = gbk2utf8(str);
		result.set(utf8);
		
		return result;
	}
	
	//gbk编码字符串转utf-8字节数组
	private byte[] gbk2utf8(String str) {  
        char c[] = str.toCharArray();  
        byte[] fullByte = new byte[3 * c.length]; 
        
        for (int i = 0,size=c.length; i < size; i++) {  
        	buff.setLength(0);
        	
            int m = (int) c[i];  
            //二进制形式
            String word = Integer.toBinaryString(m);  

            //补齐两个字节
            int len = 16 - word.length();  
            for (int j = 0; j < len; j++) {  
                buff.append("0");  
            }  
            
            buff.append(word);  
            buff.insert(0, "1110");  
            buff.insert(8, "10");  
            buff.insert(16, "10");  

            String s1 = buff.substring(0, 8);  
            String s2 = buff.substring(8, 16);  
            String s3 = buff.substring(16);          
            fullByte[i * 3] = Integer.valueOf(s1, 2).byteValue();  
            fullByte[i * 3 + 1] = Integer.valueOf(s2, 2).byteValue();  
            fullByte[i * 3 + 2] = Integer.valueOf(s3, 2).byteValue();  
        }  
        
        return fullByte;  
    }  
	

	
}
