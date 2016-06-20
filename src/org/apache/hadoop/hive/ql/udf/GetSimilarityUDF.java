package org.apache.hadoop.hive.ql.udf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 比较字符串相似度(余弦相似度)
 * 可用于比较sclick中的ua与激活数据中ua的相似性
 * hive函数签名:boolean get_similar_degree(String str1,String str2, Double sd, String split)
 * 参数1： 字符串1
 * 参数2: 字符串2
 * 参数3：相似度(0.0-1.0)
 * 参数4：分隔符
 * @author jiaqiang
 * 2014.08.04
 */
public class GetSimilarityUDF extends UDF {
	
	/**
	 * 比较两个字符串的相似度，默认分隔符为';'
	 * @param s1
	 * @param s2
	 * @return
	 */
	public double evaluate(String s1, String s2) {	
		return evaluate(s1,s2,";");
	}
	
	/**
	 * 比较两个串的相似度
	 * @param s1  字符串1
	 * @param s2  字符串2
	 * @param split 分隔符
	 * @return
	 */
	public double evaluate(String s1, String s2, String split) {	
		if (s1 == null || s2 == null) {
			return 0.0;
		}
	
		//分隔符，默认空格
		split = (split == null ? " " : split);
		
		//源串
		String origin = s1;
		int idx = origin.indexOf("(");
		if (idx != -1) { //取第一个"()"内的内容
			origin = origin.substring(idx+1);
			idx = origin.indexOf(")");
			if (idx != -1) {
				origin = origin.substring(0, idx);
			}
		}
		
		//目标串
		String target = s2;
		idx = target.indexOf("(");
		if (idx != -1) { //取第一个"()"内的内容
			target = target.substring(idx+1);
			idx = target.indexOf(")");
			if (idx != -1) {
				target = target.substring(0, idx);
			}
		}
		
		//计算相似度
		return getSimilarDegree(origin, target, split);
	}
	
	//获取字符串相似度,余弦向量
	private double getSimilarDegree(String str1, String str2, String split) {
		// 创建向量空间模型，使用map实现，主键为词项，值为长度为2的数组，存放着对应词项在字符串中的出现次数
		Map<String, int[]> vectorSpace = new HashMap<String, int[]>();
		int[] itemCountArray = null; //为了避免频繁产生局部变量，所以将itemCountArray声明在此

		// 以split为分隔符，分解字符串
		String[] strArray = str1.split(split);
		for (int i = 0,len=strArray.length; i < len; ++i) {
			//再以空格分隔字符串
			String str = strArray[i].trim();
			
			String[] arr = str.split(" ");
			for (int j = 0,len2=arr.length; j < len2; j++) {
				if (vectorSpace.containsKey(arr[j]))
					++(vectorSpace.get(arr[j])[0]);
				else {
					itemCountArray = new int[2];
					itemCountArray[0] = 1;
					itemCountArray[1] = 0;
					vectorSpace.put(arr[j], itemCountArray);
				}
			}
		}
		
		strArray = str2.split(split);
		for (int i = 0,len=strArray.length; i < len; ++i) {
			//再以空格分隔字符串
			String str = strArray[i].trim();
			String[] arr = str.split(" ");
			for (int j = 0,len2=arr.length; j < len2; j++) {
				if (vectorSpace.containsKey(arr[j]))
					++(vectorSpace.get(arr[j])[1]);
				else {
					itemCountArray = new int[2];
					itemCountArray[0] = 0;
					itemCountArray[1] = 1;
					vectorSpace.put(arr[j], itemCountArray);
				}
			}
		}
		
		// 计算相似度
		double vector1Modulo = 0.00;   // 向量1的模
		double vector2Modulo = 0.00;   // 向量2的模
		double vectorProduct = 0.00;   // 向量积
		Iterator<Entry<String, int[]>> iter = vectorSpace.entrySet().iterator();

		while (iter.hasNext()) {
			Map.Entry<String, int[]> entry = iter.next();
			itemCountArray = entry.getValue();

			vector1Modulo += itemCountArray[0] * itemCountArray[0];
			vector2Modulo += itemCountArray[1] * itemCountArray[1];

			vectorProduct += itemCountArray[0] * itemCountArray[1];
		}

		//向量模
		vector1Modulo = Math.sqrt(vector1Modulo);
		vector2Modulo = Math.sqrt(vector2Modulo);

		vectorSpace.clear();
		
		// 返回相似度
		return (vectorProduct / (vector1Modulo * vector2Modulo));
	}
	
}
