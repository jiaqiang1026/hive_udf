package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ParticipleUDF extends UDF {
	
private static Participle participle = null;
	
	static {
		if(participle == null) {
		    participle = new Participle();
		}
		participle.init("/dmp/conf/emar_ws.dict.utf8");
	}
	
	/**
	 * param entry
	 *      
	 * @return Text
	 * 
	 */

	public Text evaluate(String entry, String flag) {    
		if (entry == null) {
			return null;
		}
		if (flag == null) {
			return null;
		}
		Text text = new Text(participle.getParticiple(entry, flag));	
		return text;
	}
}
