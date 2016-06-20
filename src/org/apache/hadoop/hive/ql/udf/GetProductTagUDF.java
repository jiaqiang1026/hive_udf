package org.apache.hadoop.hive.ql.udf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 商品打标签UDF
 * 函数标签   Strnig get_product_tag(String,String)
 * @author jiaqiang
 * 2016.05.16
 */
public class GetProductTagUDF extends UDF {
	
	//key:product name  value: tag
	public static Map<String, String> dicts = new ConcurrentHashMap<String, String>(10000, 0.9f);
	
	//key:category_id value:
	public static Map<String, String> categoryMap = new ConcurrentHashMap<String, String>(3000, 0.9f);
	
	//分词树的根结点
	private static Vertex root = new Vertex(8000);
	
	static class Vertex {
        protected int words;    // 单词结束标识  0：起始字或中间字 1:单词 
        
        //key:节点字符 value:子节点
        protected Map<String,Vertex> edges; 
 
        public Vertex() {
        	this(100);
        }
        
        public Vertex(int capasity) {
            this.words = 0;
            edges = new HashMap<String,Vertex>(capasity, 0.9f);
        }
    }
	
	static {
		//加载字典&类目
		loadDicts();
		loadCategory();
		
		//初使化分词树
		init();
	}
	
	/**
	 * 给指定的商品名称打标签
	 * @param productName    商品名称
	 * @param splitChar      分隔符
	 * @return
	 */
	public String evaluate(String productName, String splitChar) {
		return getTagsToString(productName, splitChar);
	}

    /**
     * 添加分词
     * @param word
     */
    private static void addWord(String word) {
        addWord(root, word);
    }
 
    /*
     * 添加分词
     */
    private static void addWord(Vertex vertex, String word) {
        if (word.length() == 0) {
            vertex.words++;
        } else {
            char c = word.charAt(0);
            c = Character.toLowerCase(c);
            String index = String.valueOf(c);
            if (!vertex.edges.containsKey(index)) {
                vertex.edges.put(index, new Vertex(100));
            }
            
            addWord(vertex.edges.get(index), word.substring(1));
        }
    }
 
    /*
     * 获取最长匹配单词
     */
    public String getMaxMatchWord(String word) {
    	String s = "";
        String temp = "";
        char[] w = word.toCharArray();
        Vertex vertex = root;
        for (int i = 0; i < w.length; i++) {
            char c = w[i];
            c = Character.toLowerCase(c);
            String index = String.valueOf(c);
            if (!vertex.edges.containsKey(index)) {
                if (vertex.words != 0)
                    return s;
                else
                    return null;
            } else {
                if (vertex.words != 0)
                    temp = s;
                s += c;
                vertex = vertex.edges.get(index);
            }
        }
        
        if (vertex.words == 0)
            return temp;
        
        return s;
    }
    
    /**
     * 
     * @param input
     * @return  
     */
    private List<String> getTags(String input){
    	ArrayList<String> tags = new ArrayList<String>();
    	for (int i = 0; i < input.length();) {
        	String subs = input.substring(i);
        	String maxMatch = getMaxMatchWord(subs);
            if (maxMatch !=null && maxMatch.length() > 0) {
                i += maxMatch.length();
                tags.add(categoryMap.get(dicts.get(maxMatch)));
            } else {
            	i++;
            }
        }
    	
    	return tags;
    }
    
    public String getTagsToString(String input,String splittype) {
    	List<String> tags = getTags(input);
    	if (tags.size() < 1) {
    		return "-1";
    	} else {
    		StringBuffer sb = new StringBuffer();
    		for (String tag : tags) {
    			sb.append(tag);
    			sb.append(splittype);
    		}
    		
    		return sb.substring(0, sb.length()-splittype.length());
    	}
    }
    
    /*
     * 加载分词字典
     */
    private static void loadDicts() {
    	Scanner scanner = new Scanner(GetProductTagUDF.class.getResourceAsStream("/GoodsSegmentDict"));
		
    	while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			if (line.trim().length() == 0) {
				continue;
			}
			
			String[] cols = line.split(",");
			if (cols.length > 1) {
				dicts.put(cols[0].trim().toLowerCase(), cols[1].trim().toLowerCase());
			}
		}
    	
		scanner.close();
    }
    
    /*
     * 初始化分词，构建分词树
     */
    private static void init(){
    	for(String dict : dicts.keySet()){
    		addWord(dict);
    	}
    }
    
    
    /*
     * 加载商品类目
     */
    private static void loadCategory(){
    	Scanner scanner = new Scanner(GetProductTagUDF.class.getResourceAsStream("/alltags"));
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] cols = line.split(",");
			if (cols.length > 2) {
				categoryMap.put(cols[0].trim().toLowerCase(), cols[0].trim().toLowerCase());
			}
		}
		
		scanner.close();
    }
}