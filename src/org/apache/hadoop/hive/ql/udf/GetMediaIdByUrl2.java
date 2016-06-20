package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.util.HashMap;
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
 * 测试url http://www.qbaobei.com/zjbk/5_6s/jtjy/20131129_290327.html
 * select f_get_mediaid_by_url("http://www.qbaobei.com/zjbk/5_6s/jtjy/20131129_290327.html")  from xingqu;
 * select f_get_mediaid_by_url("http://www.qbaobei.com/hybk/fm/zczz/20131104_287286.html")  from xingqu;
 * select f_get_mediaid_by_url("http://bbs.xinjunshi.com/jujiao/20140410/150907.html")  from xingqu;
 * */
public class GetMediaIdByUrl2  extends UDF{ 
	
	private static String mediaTypePath = "/data/stg/common/new_url_media.md5";
	
	private Text rtn = new Text("0");
	
	private static Map<String,Map<String,String>> mediaMap = new ConcurrentHashMap<String,Map<String,String>>();
	
	private static boolean loadFlag = false;
	
	private  static final String keyIsNull=MD5(" ");//域名存在keyword为空
	
	static {
		loadFlag = load();
	}
	
	public Text evaluate(String url) {
		if (url == null) {
			return rtn;
		}
		if (loadFlag == false) {
			return rtn;
		}		
		try {
			url=url.replace("http://", "");
			String[] arr = url.split("/");
			String domain = MD5(arr[0]);   //md5
			//System.out.print(arr[0]+" : ");
			//System.out.println(MD5(domain));
			arr[arr.length-1]= arr[arr.length-1].split("[.]")[0]; //20150127
			if(mediaMap.containsKey(domain)){
				Map<String,String> keyIdMap =  mediaMap.get(domain);
				boolean containFlag=false;
				for (int i = arr.length-1; i > 0; i--) {
					//System.out.print(arr[i]+" : ");
					//System.out.println(MD5(arr[i]));
					if(keyIdMap.containsKey(MD5(arr[i]))){  //MD5
						containFlag=true;
						return new Text(keyIdMap.get(MD5(arr[i])));  //md5
					}
				}
			
			
				if(!containFlag && keyIdMap.containsKey(keyIsNull)){
					return new Text(keyIdMap.get(keyIsNull));
				}
			}
		} catch (Exception ex) {
			return rtn;
		}
		return rtn;
	}
	
	/**
	 * 从hdfs中加载(url-媒体id)文件
	 * @return
	 */
	private static boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(mediaTypePath), conf);
			in = fs.open(new Path(mediaTypePath));
			String line = null;
			while ((line = in.readLine()) != null) {
				String[] arr = line.split("\t");	
					if(arr.length==3){
						try {			
							Map<String,String> m = new HashMap<String,String>();
							if(mediaMap.containsKey(arr[0])){
								 m =mediaMap.get(arr[0]);
							}
							if("".equals(arr[1]) || null ==arr[1] || arr[1].isEmpty() || MD5("").equals(arr[1])){
								m.put(keyIsNull, arr[2]);
							}else{					
								m.put(arr[1], arr[2]);						
							}
							mediaMap.put(arr[0],m);				
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
	
	public final static String MD5(String s) {
        char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};       

        try {
            byte[] btInput = s.getBytes("UTF-8");
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
           
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
	
	public static void main(String[] args) {
		
	}
}
