package org.apache.hadoop.hive.ql.udf;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 分组topN,给分好的组打rank(序号)
 * @author jiaqiang
 * 2013.07.26
 */
public class RankUDF extends UDF {
	
	private String lastKey;
	private final AtomicInteger counter = new AtomicInteger(0);
	
	public Integer evaluate(final String key) {
		if (key == null) {
			return null;
		}
		
		if (!key.equalsIgnoreCase(this.lastKey)) {
			this.lastKey = key;
			counter.set(0);
		}
	
		return counter.addAndGet(1); 
	}
}
