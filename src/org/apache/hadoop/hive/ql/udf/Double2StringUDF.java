package org.apache.hadoop.hive.ql.udf;

import java.text.NumberFormat;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Double转字符串函数
 * @author jiaqiang
 * 2012.12.14
 */
public class Double2StringUDF extends UDF {

	private final NumberFormat nf = NumberFormat.getInstance();
	
	private Text rtn = new Text();
	
	public Text evaluate(Double d) {
		if (d == null) {
			return null;
		}
		
		try {
			nf.setMaximumFractionDigits(30);
			String s = nf.format(d);
			rtn.set(s);
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
		
		return rtn;
	}
	
	public Text evaluate(Double d, int maxFractionDigits) {
		if (d == null) {
			return null;
		}
		
		try {
			nf.setMaximumFractionDigits(maxFractionDigits);
			String s = nf.format(d);
			rtn.set(s);
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
		
		return rtn;
	}

}
