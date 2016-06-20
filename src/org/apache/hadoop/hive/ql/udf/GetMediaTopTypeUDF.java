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
 * 获取广告交换平台的顶级媒体类型函数
 * @author jiaqiang
 * 2012.10.08
 */
public class GetMediaTopTypeUDF extends UDF {
	
	private static String adxMediaTypePath = "/hive/warehouse/p_ip_dstc_map/adx_media_type.dat";
	
	private Text rtn = new Text();
	
	//广告交换平台媒体类型映射[key:类型(sub or top) value:顶级类型]
	private static Map<Integer,Integer> adxMediaTypeMap = new ConcurrentHashMap<Integer,Integer>();

	//加载成功标识
	private static boolean loadFlag = false;
	
	static {
		loadFlag = load();
	}
	
	public Text evaluate(Integer subMediaType) {
		if (subMediaType == null) {
			return null;
		}
	
		//加载媒体类型文件失败
		if (loadFlag == false) {
			return null;
		}
		
		Integer t = null;
		try {
			t = adxMediaTypeMap.get(subMediaType);
			if (t != null) {
				rtn.set(t+"");
			}
		} catch (Exception ex) {
			return null;
		}
		
		return t == null ? null : rtn;
	}
	
	/**
	 * 从hdfs中加载ip区域文件
	 * @return
	 */
	private static boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(adxMediaTypePath), conf);
			in = fs.open(new Path(adxMediaTypePath));
			String line = null;
			Integer key = null,v = null;
			
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(",");
				
				for (int i = 0,len=arr.length; i < len; i++) {
					try {
						if (i == 0) {
							key = Integer.parseInt(arr[0]);
						}
						v = Integer.parseInt(arr[i]);
						adxMediaTypeMap.put(v,key);
					} catch (Exception ex) {
						ex.printStackTrace();
						continue;
					}
				}
			} //end while
		} catch (IOException e) {
			e.printStackTrace();
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		return succ;
	}
	
}
