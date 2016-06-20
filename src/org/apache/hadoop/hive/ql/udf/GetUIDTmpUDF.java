package org.apache.hadoop.hive.ql.udf;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 从第三方数据中抽取dmp_id(一次性函数)
 * @author jiaqiang
 * 2016.04.18
 */
public class GetUIDTmpUDF extends UDF {
		 
	private Text uid = new Text("");
	
	/**
	 * 从cookie中抽取_edc参数值
	 * @param cookie
	 * @return
	 */
	public Text evaluate(String cookie) {	
		if (cookie == null || cookie.trim().length() == 0) {
			return null;
		}
		
		int sIdx = cookie.indexOf("_edc=");
		if (sIdx == -1) { //不包括参数
			return null;
		}
		
		String s = cookie.substring(sIdx+5);
		int eIdx = s.indexOf(";");
		
		try {
			uid.set((eIdx == -1 ? s.trim() : s.substring(0,eIdx).trim()));
		} catch (Exception ex) {
			return null;
		}
		
		return uid;
	}
	
}
