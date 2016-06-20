package util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * 文件操作相关工具类
 * @author jiaqiang
 * 2012.03.16
 */
public final class FileUtil {

	private static Logger log = Logger.getLogger(FileUtil.class);
	
	/**
	 * 创建目录
	 * @param path
	 * @return 创建成功或失败
	 */
	public static boolean makeDirs(File dir) {
		if (ParamUtil.isNull(dir) > 0) {
			return false;
		}
		
		boolean succ = true;
		
		if (!dir.exists()) {//目录不存在
			try {
				succ = dir.mkdirs();
				if (!succ) {//失败
					dir = null;
				}
			} catch (Exception ex) {
				succ = false;
				log.error("FileUtil::createDirs(String path)创建目录[" + dir.getAbsolutePath() + "]异常，原因[" + (ex != null ? ex.getMessage() : "") + "]");
			}
		}
		
		return succ;
	}
	
	/**
	 * 创建目录
	 * @param dir
	 * @return
	 */
	public static boolean makeDirs(String path) {
		return path == null ? false : makeDirs(new File(path));
	}
	
	/**
	 * 创建文件
	 * @param par 父级目录
	 * @param child 子目录
	 */
	public static File createFile(String par, String child) {
		if (ParamUtil.isNull(par,child) > 0) {
			return null;
		}
		
		File file = new File(par, child);
		if (!file.exists()) {
			try {
				boolean succ = file.createNewFile();
			} catch (IOException e) {
				log.error("FileUtil::createFile(String par, String child)创建文件[" + file.getAbsolutePath() + "]发生异常，原因[" + (e != null ? e.getMessage() : "") + "]");
				file = null;
			}
		}
		
		return file;
	}
	
	/**
	 * 路径代表的目录或文件是否存在
	 * @param path
	 * @return
	 */
	public static boolean exist(String path) {
		if (path == null) {
			return false;
		}
		
		File f = new File(path);
		return f.exists();
	}
	
	/**
	 * 删除目录,同时删除子目录以及子文件
	 * @param dir
	 * @return
	 */
	public static boolean deleteDir(File dir) {
		if (dir == null || !dir.exists()) {
			return true;
		}
		
		boolean succ = true;
		
		try {
			if (dir.isFile()) {
				succ = dir.delete();
			} else if (dir.isDirectory()) { //递归删除子目录与文件
				File[] fileArr = dir.listFiles();
				for (File f : fileArr) {
					succ = deleteDir(f) && succ;
				}
				
				Thread.sleep(1);
				succ = succ && dir.delete();
			}
		} catch (Exception ex) { //如权限问题或文件正在使用中
			succ = false;
		}
		
		return succ;
	}

	public static boolean deleteDir(String path) {
		return path == null ? true : deleteDir(new File(path));
	}
	
	/**
	 * 读取文本文件内容
	 * @param path
	 * @param charset
	 * @return 返回内容的所有行或null(参数为空或非法)
	 * @throws IOException 
	 */
	public static List<String> readLines(File path, String charset) throws IOException {
		if (path == null || !path.exists() || path.isDirectory())
			return null;
		
		List<String> lineList = null;
		
		BufferedReader in = null;
		
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(path), charset));
			String line = null;
			lineList = new ArrayList<String>();
			
			while ((line = in.readLine()) != null) {
				line = line.trim();
				if (line.length() == 0) {
					continue;
				}
				
				lineList.add(line);
			}
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return lineList;
	}
	
	/**
	 * 读取文本文件内容
	 * @param path
	 * @return 返回内容的所有行或null(参数为空或非法)
	 * @throws IOException 
	 */
	public static List<String> readLines(File path) throws IOException {
		return readLines(path, getEncodeCharset(path));
	}
	
	
	/**
	 * 读取文本文件内容
	 * @param path
	 * @return 返回内容的所有行或null(参数为空或非法)
	 * @throws IOException 
	 */
	public static List<String> readLines(String path) throws IOException {
		if (path == null) {
			return null;
		}
		
		File file = new File(path);
		return readLines(file, getEncodeCharset(file));
	}
	
	/**
	 * 写或追加行集合数据入文件
	 * @param dataList 行数据
	 * @param path 写入的文件(全路径)
	 * @param charset 编码方式
	 * @param append 是否追加
	 * @return
	 */
	public static boolean writeLines(Collection<String> dataList, File path, String charset, Boolean append) throws IOException {
		if (ParamUtil.isNull(dataList,path,charset,append) > 0) {
			return false;
		}
		
		PrintWriter out = null;
		FileOutputStream fos = null;
		OutputStreamWriter osw = null; 
		
		try {
			fos = new FileOutputStream(path, append);
			osw = new OutputStreamWriter(fos, charset);
			out = new PrintWriter(osw);
			
			int i = 0;
			for (String line : dataList) {
				out.println(line);
				if (i++ % 100 == 0)
					out.flush();
			}
			out.flush();
		} finally {
			if (fos != null) {
				fos.close();
			}
			if (osw != null) {
				osw.close();
			}
			if (out != null) {
				out.close();
			}
		}
		
		return true;
	}
	
	/**
	 * 写或追加行集合数据入文件
	 * @param dataList 行数据
	 * @param path 写入的文件(全路径)
	 * @param charset 编码方式
	 * @param append 是否追加
	 * @return
	 */
	public static boolean log(String data, File path, String charset, Boolean append) throws IOException {
		if (ParamUtil.isNull(data,path,charset,append) > 0) {
			return false;
		}
		
		PrintWriter out = null;
		FileOutputStream fos = null;
		OutputStreamWriter osw = null; 
		
		try {
			fos = new FileOutputStream(path, append);
			osw = new OutputStreamWriter(fos, charset);
			out = new PrintWriter(osw);
		
			out.println(data);				
			out.flush();
			out.flush();
		} finally {
			if (fos != null) {
				fos.close();
			}
			if (osw != null) {
				osw.close();
			}
			if (out != null) {
				out.close();
			}
		}
		
		return true;
	}
	
	
	/**
	 * 获取文本文件的编码
	 * @param file
	 * @return
	 * @throws IOException 
	 */
	public static String getEncodeCharset(File file) throws IOException {
		if (file == null || !file.exists() || file.isDirectory()) {
			return null;
		}
		
		InputStream in = new FileInputStream(file);
		byte[] head = new byte[3];
		in.read(head);
		
		//默认
		String charset = "gbk";
		if (head[0] == -1 && head[1] == -2 )  
			charset = "UTF-16";  
        if (head[0] == -2 && head[1] == -1 )  
        	charset = "Unicode";  
        if(head[0]==-17 && head[1]==-69 && head[2] ==-65)  
        	charset = "UTF-8";  
        
		return charset; 
	}
	
	public static String getEncodeCharset(String path) throws IOException {
		if (path == null)
			return null;
		
		return getEncodeCharset(new File(path));
	}
	
	
	/**
	 * 文件拷贝(字节拷贝实现)
	 * @param source
	 * @param target
	 * @param buffSize 缓存大小(byte)
	 * @return
	 * @throws IOException
	 */
	public static boolean copy(File source, File target, int buffSize) throws IOException {
		if (source == null || target == null || source.isDirectory() || target.isDirectory())
			return true;
		
		BufferedInputStream in = null;
		FileInputStream fis = null;
		FileOutputStream out = null;
		
		try {
			byte[] buff = new byte[buffSize];
			fis = new FileInputStream(source);
			in = new BufferedInputStream(fis);
			out = new FileOutputStream(target, true);
			
			int len = -1;
			while ((len = in.read(buff)) != -1) {
				out.write(buff, 0, len);
			}
			out.flush();
		} finally {
			if (fis != null) {
				fis.close();
			}
			if (in != null) {
				in.close();
			}
			if (out != null) {
				out.close();
			}
		}
		
		return true;
	}
	
	/**
	 * 文件拷贝(字节拷贝)
	 * @param source
	 * @param target
	 * @return
	 * @throws IOException 
	 */
	public static boolean copy(File source, File target) throws IOException {
		return copy(source, target, 10240);
	}
	
	/**
	 * 读取文件的有效行数,不包含空行
	 * @param file
	 * @param charset
	 * @return
	 * @throws IOException
	 */
	public static Integer lineCount(File file, String charset) throws IOException {
		BufferedReader in = null;
		Integer count = 0;
		
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
			String line = null;
			while ((line = in.readLine()) != null) {
				if (line.trim().length() != 0) //非空
					count++;
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}
		
		return count;
	}
	
	/**
	 * 文件|目录重命名
	 * @param sourceFile
	 * @param targetFile
	 * @return
	 */
	public static boolean rename(File sourceFile, File targetFile) {
		if (sourceFile == null || targetFile == null)
			return false;
		
		return sourceFile.renameTo(targetFile);
	}	
}
