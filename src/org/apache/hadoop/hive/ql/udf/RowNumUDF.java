package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author jiaqiang
 * 2012.06.12
 */
public class RowNumUDF extends UDF {
	
	private Integer rownum = 1;
	
	public Integer evaluate(final Text s) {
		return rownum++;
	}
}
