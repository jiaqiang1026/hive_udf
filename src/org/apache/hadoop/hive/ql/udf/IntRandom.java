package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

public class IntRandom extends UDF {
	
	private IntWritable rtn = new IntWritable();
	
	public IntWritable evaluate() {

		try {
			int i = (int) (Math.random() * 100) + 1;
			rtn.set(i);
		} catch (Exception e) {
			rtn.set(9999);
		}
		
		return rtn;
	}

}
