package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * ȡ�ַ����Ӵ����Էָ���Ϊ��־,ȡ�̶��������Ӵ�,��A,B,C,D -substring('A,B,C,D',',',3)--->A,B,C
 * @author jiaqiang
 * 2012.10.08
 */
public class SubStringUDF extends UDF {
	
	private Text rtn = new Text();
	
	
	/**
	 * ȡ�Ӵ�
	 * @param src Դ��
	 * @param delimiter �ַ����ķָ���
	 * @param count �Ӵ�����
	 * @param connector �����Ӵ������ӷ�
	 * @return
	 */
	public Text evaluate(String src, String delimiter, int fromIdx, int endIdx, String connector) {
		if (src == null) {
			return null;
		}
		
		String str = src.toString().trim();
		if (str.length() == 0) {
			return null;
		}
		connector = (connector == null ? "," : connector);
		try {
			String[] arr = str.split(delimiter);
			String t = "";
				
			for (int i = fromIdx; i < endIdx; i++) {
				t += (i != endIdx-1 ? arr[i]+connector : arr[i]);
			}
			
			rtn.set(t);
		} catch (Exception ex) {
			return null;
		}
		
		return rtn;
	}
	
	
	/**
	 * ȡ�Ӵ�
	 * @param src Դ��
	 * @param delimiter �ַ����ķָ���
	 * @param count �Ӵ�����
	 * @param connector �����Ӵ������ӷ�
	 * @return
	 */
	public Text evaluate(String src, String delimiter, int count, String connector) {
		if (src == null) {
			return null;
		}
		
		String str = src.toString().trim();
		if (str.length() == 0) {
			return null;
		}
		connector = (connector == null ? "," : connector);
		try {
			String[] arr = str.split(delimiter);
			String t = "";

			if (count > arr.length) {
				for (int i = 0,len=arr.length; i < len; i++) {
					t += (i != len-1 ? arr[i]+delimiter : arr[i]);
				}
			} else {
				for (int i = 0; i < count; i++) {
					t += (i != count-1 ? arr[i]+connector : arr[i]);
				}
			}
			
			rtn.set(t);
		} catch (Exception ex) {
			return null;
		}
		
		return rtn;
	}
	
	/**
	 * ȡ�Ӵ�
	 * @param src Դ��
	 * @param delimiter �ַ����ķָ���
	 * @param count �Ӵ�����
	 * @param connector �����Ӵ������ӷ�
	 * @return
	 */
	public Text evaluate(String src, String delimiter, int count) {
		return evaluate(src,delimiter,count,delimiter);
	}
	
	public static void main(String[] args) {
		String ip = "127.0.0.1";
		//System.out.println(ip.split("[.]").length);
		SubStringUDF u = new SubStringUDF();
		System.out.println(u.evaluate(ip,"\\.",3,"."));
		
	}
}
