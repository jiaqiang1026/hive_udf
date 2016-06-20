package test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;



public class Test {

	//退出页信息封装
	static class ExitPage implements Comparable {
		String sid;           //网站id
		long siteFirstTime;   //网站出现的最早时间
		String url;           //页面url,用md5值表示
		long time;            //页面浏览的时间
		
		public ExitPage(long siteFirstTime) {
			this.siteFirstTime = siteFirstTime;
		}
		
		@Override
		public int compareTo(Object obj) {
			ExitPage ep = (ExitPage) obj;
			return (int) (siteFirstTime - ep.siteFirstTime);
		}
		
		public String toString() {
			return this.siteFirstTime + "";
		}
	}
	
	public static void main(String[] args) {
		List<ExitPage> list = new ArrayList<ExitPage>();
		list.add(new ExitPage(10));
		list.add(new ExitPage(9));
		list.add(new ExitPage(8));
		list.add(new ExitPage(12));
		
		System.out.println(list);
		
		Collections.sort(list);
		
		System.out.println(list);
		
		String s = "L1_10.5|L2_3";
		String[] arr = s.split("[|]");
		for (String ele : arr) {
			System.out.println(ele);
		}
		
		System.out.println(s.indexOf("."));
		
	}

}
