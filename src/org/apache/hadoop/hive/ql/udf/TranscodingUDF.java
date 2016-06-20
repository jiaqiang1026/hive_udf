package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * �ַ���ת�뺯��
 * @author jiaqiang
 * 2013.04.09
 */
public class TranscodingUDF extends UDF {

	private Text result = new Text();
	
	private StringBuffer buff = new StringBuffer();  
	
	/**
	 * ��ԭ��encodeCharset�ַ������ַ���ת����targetCharset���ַ���
	 * @param str
	 *           ������ַ���
	 * @param charset
	 *           �ַ���ԭ�ȵ��ַ���
	 * @param targetCharset
	 *           Ҫת���������ַ���
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
	 * ��ԭ��encodeCharset�ַ������ַ���ת����targetCharset���ַ���
	 * @param str
	 *           ������ַ���
	 * @param charset
	 *           �ַ���ԭ�ȵ��ַ���
	 * @param targetCharset
	 *           Ҫת���������ַ���
	 * @return
	 */
	public Text evaluate(String str, String charset) {
		return evaluate(str, charset, "utf-8");
	}
	
	/**
	 * Ĭ��gbk���룬Ĭ��תudf-8����
	 * @param str
	 *           ������ַ���
	 */
	public Text evaluate(String str) {
		if (str == null) {
			return null;
		}
		
		byte[] utf8 = gbk2utf8(str);
		result.set(utf8);
		
		return result;
	}
	
	//gbk�����ַ���תutf-8�ֽ�����
	private byte[] gbk2utf8(String str) {  
        char c[] = str.toCharArray();  
        byte[] fullByte = new byte[3 * c.length]; 
        
        for (int i = 0,size=c.length; i < size; i++) {  
        	buff.setLength(0);
        	
            int m = (int) c[i];  
            //��������ʽ
            String word = Integer.toBinaryString(m);  

            //���������ֽ�
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
