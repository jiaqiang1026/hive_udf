package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 获取广告位尺寸大小函数,取宽高最大值，判断范围并标准化为1,2,3，对应大中小
 * @author jiaqiang
 * 2012.11.08
 */
public class GetAdSiteSizeUDF extends UDF {
	
	private Text rtn = new Text();
	
	/**
	 * 取宽高最大值，(0,300)为3(小),[300,700)为2(中) [700,)为1(大) 
	 * @param width
	 * @param height
	 * @return
	 */
	public Text evaluate(Integer width, Integer height) {
		if (width == null || height == null) {
			return null;
		}
	
		Integer max = (width > height ? width : height);
		
		if (max < 300) { //小
			rtn.set(3+"");
		} else if (max < 700) { //中
			rtn.set(2+"");
		} else { //大
			rtn.set(1+"");
		}
		
		return rtn;
	}
	

}
