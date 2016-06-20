package org.apache.hadoop.hive.ql.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 根据URL获取广告投放域名(主或子域名)
 * 对应hive函数名称为get_adput_domain,参数为url
 * @author jiaqiang
 * 2013.06.26
 */
public class GetAdPutDomainUDF extends UDF {
	
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
		
		String t = getAdputDomainName(url);
		if (t != null) {
			domain.set(t);
		}
		
		return t == null ? null : domain;
	}

	/**
	 * 解析url域名(主或是子)
	 * @param url
	 * @return null或域名
	 */
	private synchronized String getAdputDomainName(String url) {
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
				return url.substring(0,idx);
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
		//String url = "http://www.xitie.com/dangdang.php?no=1000059203";
		String url = "http://abc.news.xitie.com/test";
		GetAdPutDomainUDF udf = new GetAdPutDomainUDF();
		System.out.println(udf.getAdputDomainName(url));
	}
}
