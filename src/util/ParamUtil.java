package util;

/**
 * 参数工具类
 * @author jiaqiang
 * 2012.02.22
 */
public class ParamUtil { 
		
	/**
	 * 判断参数中是否有空值
	 * @param params
	 * @return 返回空值参数的下标.-1:没有空值, >0:空值参数下标
	 */
	public static int isNull(Object... params) {
		if (params == null) {
			return 1;
		}
		
		for (int i = 0,size=params.length; i < size; i++) {
			if (params[i] == null) {
				return (i+1);
			}
		}
		
		return -1;
	}

}
