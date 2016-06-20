package org.apache.hadoop.hive.ql.udf;

public class Participle {

	static {
		System.loadLibrary("participle");		
	}

	/**
	 * param path
	 * 
	 * @return
	 */
	public native void init(String path);

	/**
	 * param str
	 * 
	 * @return String like A,B,C,D...
	 */
	public native String getParticiple(String str, String flag);
}
