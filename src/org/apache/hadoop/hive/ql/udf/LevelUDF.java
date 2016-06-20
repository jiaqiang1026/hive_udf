package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 判断数字所属分段
 * 函数签名: string level(init_value,step,max_value,value)
 * 如：level(0,0.5,1)=L2
 *    level(0,0.3,0.2)=L1
 * @author jiaqiang
 * 2015.08.25
 */
public class LevelUDF extends UDF {
	
	public String evaluate(Double init, Double step, Double max, Double v) {
		if (v == null) {
			return "L0";
		}
	
		int maxLevel = (int)((max-init)/step);
		int level = (int) ((v-init)/step);
		
		return (level >= maxLevel ? "L" + maxLevel : "L" + level);
	}
}
