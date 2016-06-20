package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * ȡ�ַ����Ӵ����Էָ���Ϊ��־,ȡ�̶��������Ӵ�,��A,B,C,D -substring('A,B,C,D',',',3)--->A,B,C
 * @author jiaqiang
 * 2012.10.08
 */
public class StationUDF extends UDF {
	
	private Text rtn = new Text();
	
	
	/**
	 * ȡ�Ӵ�
	 * @param src Դ��
	 * @param delimiter �ַ����ķָ���
	 * @param count �Ӵ�����
	 * @param connector �����Ӵ������ӷ�
	 * @return
	 */
	public Text evaluate(final String station) {
		String s = station;
		
		int idx = station.indexOf(".");
		if (idx != -1) {
			s = station.substring(0,idx);
		}
		
		idx = s.indexOf("|");
		if (idx != -1) {
			s = s.substring(0,idx);
		}
		
		rtn.set(s);
		return rtn;
	}
}
