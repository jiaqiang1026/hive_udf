package org.apache.hadoop.hive.ql.udf;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 获取给定url|referer主域名
 * 函数1: string get_domain(string)
 * 参数：url or referer
 * 返回：主域名或null
 * 函数2: string get_domain(string,boolean)
 * 参数：
 * param 1:url or referer 
 * param 2:是否对参数1进行url decode
 * @author jiaqiang
 * 2012.02.19
 */
public class GetMainDomainUDF2 extends UDF {
	
	private Text domain = new Text();
	
	//国家域名map
	private static Map<String,String> nationMap = new HashMap<String,String>();
	
	//顶级域名map
	private static Map<String,String> topDomainMap = new HashMap<String,String>();
	
	private String ipPattern = "(2[5][0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})";
	
	static {
		init();
	}
	
	public Text evaluate(String url) {
		if (null == url) {
			return null;
		}

		url = url.trim();
		if (url.length() == 0) {
			return null;
		}
		
		String t = getDomainName(url);
		if (t != null) {
			domain.set(t);
		}
		
		return t == null ? null : domain;
	}

	public Text evaluate(String url,boolean decode) {
		if (null == url) {
			return null;
		}

		url = url.trim();
		if (url.length() == 0) {
			return null;
		}
		
		if (decode) {
			url = URLDecoder.decode(url);
		}
		
		String t = getDomainName(url);
		if (t != null) {
			domain.set(t);
		}
		
		return t == null ? null : domain;
	}
	
	/**
	 * 解析url的主域名
	 * @param url
	 * @return null或域名
	 */
	private synchronized String getDomainName(String url) {
		//必须以http://或https://开头
		if (!url.startsWith("http://") && !url.startsWith("https://")) {
			return null;
		}
	
		String domain = null;
		
		//去删除协议前缀
		url = url.replace("http://", "").replace("https://", "");
		int idx = url.indexOf("/");
	
		//bbs.egou.com/club/?from=jfbtc
		if (url.startsWith("www.")) { //"www."开头
			domain = (idx == -1) ? url.replace("www.", "") : url.substring(0, idx).replace("www.", "");
		} else { //不以www开头，解析各个域名
			if (idx != -1) {
				url = url.substring(0,idx);
			}
			
			//ip点分
			int i = -1;
			if ((i = url.indexOf(":")) != -1) { //
				String ip = url.substring(0,i);
				if (ip.matches(ipPattern)) {
					domain = ip + url.substring(i,url.length());
				}
			} else {
				String[] arr = url.split("\\.");
				for (i = arr.length-1; i >= 0; i--) {
					String v = arr[i];				
					domain = (i == arr.length-1) ? v : v + "." + domain;
					
					if (i == arr.length-1 || nationMap.containsKey(v) || topDomainMap.containsKey(v)) {
						continue;
					} 
					
					break;
				} //end for
			}
		}
		
		return domain;
	}

	private static void init() {
		String nation = "ad,ae,af,ag,ai,al,am,ao,ar,at,au,az,bb,bd,be,bf,bg,bh,bi,bj,bl,bm,bn,bo,br,bs,bw,by,bz,ca,cf,cg,ch,ck,cl,cm,cn,co,cr,cs,cu,cy,cz,de,dj,dk,do,dz,ec,ee,eg,es,et,fi,fj,fr,ga,gb,gd,ge,gf,gh,gi,gm,gn,gr,gt,gu,gy,hk,hn,ht,hu,id,ie,il,in,iq,ir,is,it,jm,jo,jp,ke,kg,kh,kp,kr,kt,kw,kz,la,lb,lc,li,lk,lr,ls,lt,lu,lv,ly,ma,mc,md,mg,ml,mm,mn,mo,ms,mt,mu,mv,mw,mx,my,mz,na,ne,ng,ni,nl,no,np,nr,nz,om,pa,pe,pf,pg,ph,pk,pl,pr,pt,py,qa,ro,ru,sa,sb,sc,sd,se,sg,si,sk,sl,sm,sn,so,sr,st,sv,sy,sz,td,tg,th,tj,tm,tn,to,tr,tt,tw,tz,ua,ug,us,uy,uz,vc,ve,vn,ye,yu,za,zm,zr,zw";
		String top = "ac,ah,biz,bj,cc,com,cq,edu,fj,gd,gov,gs,gx,gz,ha,hb,he,hi,hk,hl,hn,info,io,jl,js,jx,ln,mo,mobi,net,nm,nx,org,qh,sc,sd,sh,sn,sx,tj,tm,travel,tv,tw,ws,xj,xz,yn,zj";
		
		String[] arr = nation.split(",");
		for (String s : arr) {
			nationMap.put(s, s);
		}
		
		arr = top.split(",");
		for (String s : arr) {
			topDomainMap.put(s, s);
		}
	}
	
	public static void main(String[] args) {
		//String url = "http://zgjp360-969935.adminkc.cn/test.html";
		String url = "http%3A%2F%2Fbbs.xinjunshi.com%2Fjujiao%2F20140614%2F163802.html";
		GetMainDomainUDF2 udf = new GetMainDomainUDF2();
		System.out.println(udf.evaluate(url,true));
		
		String s = "http%3A%2F%2Fwww.yzw19.com%2Fwuqitupian%2F2013%2F0717%2F22245_4.html";
		System.out.println(URLDecoder.decode(s));
	}
}
