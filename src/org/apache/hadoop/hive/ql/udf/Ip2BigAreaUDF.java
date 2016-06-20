package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

/**
 * 将直接区域编码映射成4大区域编码
 * 1--网民少的区域包括：内蒙古、新疆、吉林、陕西、甘肃、西藏、贵州、宁夏、青海
 * 2--网民用户量中等区域包括：安徽、湖北、江西、云南、黑龙江、山西、海南、河南、福建、湖南、广西、四川、河北
 * 3--网民数量多区域包括：重庆、天津、上海、辽宁、北京、山东、浙江、广东、江苏
 * 4--网民数量很小区域：港、澳、台̨
 * @author jiaqiang
 * 2012.11.7
 */
public final class Ip2BigAreaUDF extends UDF {
	
	//区域编码文件路径
	private static String areaCodePath = "/hive/warehouse/p_ip_dstc_map/area_mn.dat";
	
	//区域映射[key:区域编码  value:父级区域编码]
	private static Map<Integer, Integer> areaCodeMap = new ConcurrentHashMap<Integer, Integer>();
	
	//key:省区域编码  value:划分的4大区域编码
	private static Map<Integer, Integer> province2BigAreaCodeMap = new ConcurrentHashMap<Integer, Integer>();
	
	//初使化成功标识
	private static boolean initSucc = false;
	
	//返回的大的区域编码
	private Text result = new Text();
	
	static {
		init();
		boolean succ = load();
		if (succ) {
			initSucc = true;
		}
	}
	
	/**
	 * @param areaCode 传入的区域编码
	 * @return
	 */
	public Text evaluate(Text areaCode) {
		if (areaCode == null) {
			return null;
		}
		
		if (!initSucc) { //初使化失败
			return null;
		}
		
		String acStr = areaCode.toString().trim();
		if (acStr.length() == 0) {
			return null;
		}
		
		Integer bigAreaCode = null;
		
		try {
			Integer ac = Integer.parseInt(acStr);
			//判断是否是省级区域
			bigAreaCode = province2BigAreaCodeMap.get(ac);
			if (bigAreaCode != null) {
				result.set(bigAreaCode+"");
				return result;
			} else { //二级区域
				//获取父级区域编码
				Integer parAreaCode = areaCodeMap.get(ac);
				bigAreaCode = province2BigAreaCodeMap.get(parAreaCode);
			}
		} catch (Exception ex) {
			return null;
		}
		
		if (bigAreaCode != null) {
			result.set(bigAreaCode+"");
		}
		
		return bigAreaCode == null ? null : result;
	}
	
	/**
	 * @param areaCode 传入的区域编码(由f_ip2dstc函数传入)
	 * @param regionCode 行政区域编码
	 * @return
	 */
	public Text evaluate(Text areaCode, Text regionCode) {
		if (areaCode == null && regionCode == null) {
			return null;
		}
		
		if (!initSucc) { //初使化失败
			return null;
		}
		
		//先处理ip转过来的区域编码
		Text t = evaluate(areaCode);
		if (t != null) {
			return t;
		}
		
		//无效ip,根据行政区域编码,只处理google adx		
		if (regionCode == null) { //adx给出的中国行政区域为空
			return null;
		}
		
		String rc = regionCode.toString().trim();
		if (rc.length() == 0) {
			return null;
		}
		if (!rc.startsWith("CN-")) {//google adx给出的行政区域编码以'CN-'开头
			return null;
		}
		
		rc = rc.replace("CN-", "").replace("cn-", "");
		int len = rc.length();
		for (int i = 1; i <= (6-len); i++) {
			rc += "0";
		}
		
		Integer bigAreaCode = null;
		
		try {
			Integer ac = Integer.parseInt(rc);
			//判断是否是省级区域
			bigAreaCode = province2BigAreaCodeMap.get(ac);
			if (bigAreaCode != null) {
				result.set(bigAreaCode+"");
				return result;
			} else { //二级区域
				//获取父级区域编码
				Integer parAreaCode = areaCodeMap.get(ac);
				bigAreaCode = province2BigAreaCodeMap.get(parAreaCode);
			}
		} catch (Exception ex) {
			return null;
		}
		
		if (bigAreaCode != null) {
			result.set(bigAreaCode+"");
		}
		
		return bigAreaCode == null ? null : result;
	}
	
	
	/**
	 * 加载区域编码
	 * @return
	 */
	private static boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(areaCodePath), conf);
			in = fs.open(new Path(areaCodePath));
			String line = null;
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				
				try {
					Integer areaCode = Integer.parseInt(arr[0].trim());
					Integer parAreaCode = Integer.parseInt(arr[2].trim());
					areaCodeMap.put(areaCode, parAreaCode);
				} catch (Exception ex) {
					continue;
				}
			}
		} catch (IOException e) {
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		return succ;
	}

	//加入省级区域到大区域的映射
	private static void init() {
		//1)加载一级区域下的省或自治区
		
		//内蒙古自治区
		province2BigAreaCodeMap.put(150000, 1);
		//新疆维吾尔自治区
		province2BigAreaCodeMap.put(650000, 1);
		//吉林
		province2BigAreaCodeMap.put(220000, 1);
		//陕西
		province2BigAreaCodeMap.put(610000, 1);
		//甘肃
		province2BigAreaCodeMap.put(620000, 1);
		//西藏
		province2BigAreaCodeMap.put(540000, 1);
		//贵州
		province2BigAreaCodeMap.put(520000, 1);
		//宁夏
		province2BigAreaCodeMap.put(640000, 1);
		//青海
		province2BigAreaCodeMap.put(630000, 1);
		
		//2)加载二级区域下的省或自治区
		//安徽
		province2BigAreaCodeMap.put(340000, 2);
		//湖北
		province2BigAreaCodeMap.put(420000, 2);
		//江西
		province2BigAreaCodeMap.put(360000, 2);
		//云南
		province2BigAreaCodeMap.put(530000, 2);
		//黑龙江
		province2BigAreaCodeMap.put(230000, 2);
		//山西
		province2BigAreaCodeMap.put(140000, 2);
		//海南
		province2BigAreaCodeMap.put(460000, 2);
		//河南
		province2BigAreaCodeMap.put(410000, 2);
		//福建
		province2BigAreaCodeMap.put(350000, 2);
		//湖南
		province2BigAreaCodeMap.put(430000, 2);
		//广西
		province2BigAreaCodeMap.put(450000, 2);
		//四川
		province2BigAreaCodeMap.put(510000, 2);
		//河北
		province2BigAreaCodeMap.put(130000, 2);
		
		//3)加载三级区域下的省或自治区
		//重庆
		province2BigAreaCodeMap.put(500000, 3);
		//天津
		province2BigAreaCodeMap.put(120000, 3);
		//上海
		province2BigAreaCodeMap.put(310000, 3);
		//辽宁
		province2BigAreaCodeMap.put(210000, 3);
		//北京
		province2BigAreaCodeMap.put(110000, 3);
		//山东
		province2BigAreaCodeMap.put(370000, 3);
		//浙江
		province2BigAreaCodeMap.put(330000, 3);
		//广东
		province2BigAreaCodeMap.put(440000, 3);
		//江苏
		province2BigAreaCodeMap.put(320000, 3);
		
		//4)加载四级区域下的省或自治区
		//台湾
		province2BigAreaCodeMap.put(710000, 4);
		//香港
		province2BigAreaCodeMap.put(810000, 4);
		//澳门
		province2BigAreaCodeMap.put(820000, 4);
	}
	
	//处理区域编码
	private Text process(Text areaCode) {
		return evaluate(areaCode);
	}
	
}
