package org.apache.hadoop.hive.ql.udf;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 获取广告创意类型函数，创意类型[flash,image,text,video]分别标准化为[1,2,3,4]
 * @author jiaqiang
 * 2012.11.08
 */
public class GetIdeaTypeUDF extends UDF {
	
	private Text rtn = new Text();
	
	private boolean init = false;
	
	private Map<String, Integer> ideaTypeMap;
	
	/**
	 * 标准化创意类型
	 * @param ideaType 创意类型的字符串表示
	 * @return
	 */
	public Text evaluate(Text ideaType) {
		if (ideaType == null) {
			return null;
		}	
		
		//空串
		String sIdeaType = ideaType.toString().trim();
		if (sIdeaType.length() == 0) {
			return null;
		}
		
		if (!init) {
			init();
			init = true;
		}
		
		Integer t = null;
		try {
			t = ideaTypeMap.get(sIdeaType);
			if (t != null) {
				rtn.set(t+"");
			}
		} catch (Exception ex) {
			return null;
		}
		
		return t == null ? null : rtn;
	}
	
	/**
	 * 初使化，加入创意类型映射
	 */
	private void init() {
		if (ideaTypeMap == null) {
			ideaTypeMap = new ConcurrentHashMap<String,Integer>();
		}
		
		ideaTypeMap.put("flash", 1);
		ideaTypeMap.put("image", 2);
		ideaTypeMap.put("text", 3);
		ideaTypeMap.put("video", 4);
	}
}
