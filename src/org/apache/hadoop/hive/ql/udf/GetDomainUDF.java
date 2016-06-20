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
 * 2016.03.09
 */
public class GetDomainUDF extends UDF {
	
	private Text domain = new Text("-1");
	
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
		
		String t = getDomainName(url,0);
		if (t != null) {
			domain.set(t);
		}
		
		return t == null ? null : domain;
	}

	public Text evaluate(String url, int subDomain, boolean decode) {
		if (null == url) {
			return null;
		}

		url = url.trim();
		if (url.length() == 0) {
			return null;
		}
		
		if (decode) {
			try {
				url = URLDecoder.decode(url);
			} catch (Exception ex) {
				ex.printStackTrace();
				return null;
			}
		}
		
		String t = getDomainName(url,subDomain);
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
	private synchronized String getDomainName(String url, int subDomain) {
		//必须以http://或https://开头
		if (!url.startsWith("http://") && !url.startsWith("https://")) {
			return null;
		}
	
		if (subDomain <= 0) {
			subDomain = 1;
		}
		
		String domain = "";
		
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
			if ((i = url.indexOf(":")) != -1) { //带有端口号
				String ip = url.substring(0,i);
				if (ip.matches(ipPattern)) { // ip+port
					domain = ip + url.substring(i,url.length());
					return domain;
				} 
			}
			
			url = (i != -1 ? url.substring(0,i) : url);
			if (url.matches(ipPattern)) {
				return url;
			} else {
				String[] arr = url.split("\\.");
				if (arr.length <= 1) {
					return null;
				}
				
				for (int j = 0; j < subDomain; j++) {
					if (j >= arr.length) {
						break;
					}
					
					String v = arr[arr.length-1-j];
					domain = (j == 0) ? v : v + "." + domain;
					
					if (nationMap.containsKey(v) || topDomainMap.containsKey(v)) {
						subDomain++;
					}
				}
			}
		}
		
		return domain;
	}

	private static void init() {
		String nation = "ad,ae,af,ag,ai,al,am,ao,ar,at,au,az,bb,bd,be,bf,bg,bh,bi,bj,bl,bm,bn,bo,br,bs,bw,by,bz,ca,cf,cg,ch,ck,cl,cm,cn,co,cr,cs,cu,cy,cz,de,dj,dk,do,dz,ec,ee,eg,es,et,fi,fj,fr,ga,gb,gd,ge,gf,gh,gi,gm,gn,gr,gt,gu,gy,hk,hn,ht,hu,id,ie,il,in,iq,ir,is,it,jm,jo,jp,ke,kg,kh,kp,kr,kt,kw,kz,la,lb,lc,li,lk,lr,ls,lt,lu,lv,ly,ma,mc,md,mg,ml,mm,mn,mo,ms,mt,mu,mv,mw,mx,my,mz,na,ne,ng,ni,nl,no,np,nr,nz,om,pa,pe,pf,pg,ph,pk,pl,pr,pt,py,qa,ro,ru,sa,sb,sc,sd,se,sg,si,sk,sl,sm,sn,so,sr,st,sv,sy,sz,td,tg,th,tj,tm,tn,to,tr,tt,tw,tz,ua,ug,us,uy,uz,vc,ve,vn,ye,yu,za,zm,zr,zw";
		String top = "ac,ah,biz,bj,cc,com,cq,edu,fj,gd,gov,gs,gx,gz,ha,hb,he,hi,hk,hl,hn,info,io,jl,js,jx,ln,mo,mobi,net,nm,nx,org,qh,sc,sd,sh,sn,sx,tj,tm,travel,tv,tw,ws,xj,xz,yn,zj,pw,cc,me,mx";
		
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
		GetDomainUDF udf = new GetDomainUDF();
//		String s = "http://139.129.106.102/if/dalian_if.php?w=200&h=500&type=taobaostrip&id=dalian_if&url=http%3A%2F%2Fwww.ifeng.com%2Fssi-incs%2Fdemo%2Fwangzhan-shouye-juxing03-taobao510-150123.html";
//		String s = "http://jss.tenglink.com/rb/tmp/js/31260.html";
//		String s = "http://www.newfeng.cn/a/view/opinion/2015/0206/30.html";
//		String s = "https://m.baidu.com/from=0/bd_page_type=1/ssid=0/uid=0/pu=usm%400%2Csz%401320_1001%2Cta%40iphone_";
//		String s = "calcuworld.com";
//		String s = "http://sports.qq.com/a/20160517/073887.htm?pgv_ref=aio2015&ptlang=2052";
		String s = "http://abc";
		System.out.println(udf.evaluate(s,1,true));
	}
}
