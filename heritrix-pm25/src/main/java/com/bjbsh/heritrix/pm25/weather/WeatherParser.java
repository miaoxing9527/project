package com.bjbsh.heritrix.pm25.weather;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.util.NodeList;

public class WeatherParser {

	final static String[] links = {
			"http://www.weather.com.cn/textFC/hb.shtml",
			"http://www.weather.com.cn/textFC/db.shtml",
			"http://www.weather.com.cn/textFC/hd.shtml",
			"http://www.weather.com.cn/textFC/hz.shtml",
			"http://www.weather.com.cn/textFC/hn.shtml",
			"http://www.weather.com.cn/textFC/xb.shtml",
			"http://www.weather.com.cn/textFC/xn.shtml",
			"http://www.weather.com.cn/textFC/gat.shtml",
			};
	
	
	
	public static void main(String[] args) {

		for(int i = 0 ; i < links.length; i++) {
			parser(links[i]);
		}
	}
	
	public static List<Document> parser() {
		
		List<Document> result = new ArrayList<Document>();
		
		for(int i = 0 ; i < links.length; i++) {
			List<Document> list = parser(links[i]);
			if(null != list && !list.isEmpty()) {
				result.addAll(list);
			}
		}
		
		return result;
	}
	
	public static List<Document> parser(String url) {
		
		List<Document> result = new ArrayList<Document>();
		String today = today();
		
		try {
			// 1、构造一个Parser，并设置相关的属性  
	        Parser parser = new Parser(url);
	        parser.setEncoding("gb2312"); 
	        
	        // 2.1、自定义一个Filter，用于过滤<Frame >标签，然后取得标签中的src属性值  
            NodeFilter frameNodeFilter = new NodeFilter() {  
                @Override  
                public boolean accept(Node node) {  
                	
                	if(node.getText().startsWith("table")) {
                		Node parent = node.getParent();
                		if(null == parent) {
                			return false;
                		}
                		
                		Node pparent = parent.getParent();
                		if(null == pparent) {
                			return false;
                		}
                		
                		if (pparent.getText().contains("conMidtab") && !pparent.getText().contains("display:none;")) { 
                            return true;  
                        } 
                	}

                    return false;  
                }  
            }; 
            
            // 3、使用parser根据filter来取得所有符合条件的节点  
            NodeList nodeList = parser.extractAllNodesThatMatch(frameNodeFilter); 
            for(int i = 0; i<nodeList.size();i++){  
                Node node = nodeList.elementAt(i);
                
                NodeList trs = node.getChildren();
                
                String province = "";
                
                if(trs.size() > 2) {
                	for(int j = 0; j < trs.size(); j++) {
                		if(j < 2) {
                			continue;
                		}
                		
                		Node tr = trs.elementAt(j);
                		NodeList tds = tr.getChildren();
                		int tdsize = tds.size();
                		
                		if(j == 2 && tdsize == 9) {
                			province = tds.elementAt(0).getFirstChild().getFirstChild().getText();
//                    		System.out.println("@@@@@@@@@" + province + "@@@@@@@@@");
                		}
                		
                		int index_city = 0;
                		if(tdsize == 9) {
                			index_city++;
                		}
                		
                		String city = tds.elementAt(index_city).getFirstChild().getFirstChild().getText();
                		String day_txt = tds.elementAt(index_city+1).getFirstChild().getText();
                		String day_wind = getWindString(tds.elementAt(index_city+2));
                		String day_tmp_hight = tds.elementAt(index_city+3).getFirstChild().getText();
                		String night_txt = tds.elementAt(index_city+4).getFirstChild().getText();
                		String night_wind = getWindString(tds.elementAt(index_city+5));
                		String night_tmp_low = tds.elementAt(index_city+6).getFirstChild().getText();
                		
//                		String json = "{key:" + today + ",province:" + province + ",city:" + city + ",day_txt:" + day_txt + ",day_wind:"
//                		+ day_wind  + ",night_txt:" + night_txt + ",night_wind:" + night_wind + ",tmp_hight:" + day_tmp_hight + ",tmp_low:" + night_tmp_low + "}";
//                		
                		
                		Document doc = new Document();
                		doc.append("key", today);
                		doc.append("province", province);
                		doc.append("city", city);
                		doc.append("day_txt", day_txt);
                		doc.append("day_wind", day_wind);
                		doc.append("night_txt", night_txt);
                		doc.append("night_wind", night_wind);
                		doc.append("tmp_hight", day_tmp_hight);
                		doc.append("tmp_low", night_tmp_low);
                		
                		result.add(doc);
                		
//                		System.out.println(doc);

                	}
                }  
            }
            
		} catch (Exception e) {

			e.printStackTrace();
		}  
        
		return result;
	}

	public static String getWindString(Node windNode) {
		StringBuffer buffer = new StringBuffer();
		NodeList nodes = windNode.getChildren();
		for(int j = 0; j < nodes.size(); j++) {
			buffer.append(nodes.elementAt(j).getFirstChild().getText());
			buffer.append(" ");
		}
		
		return new String(buffer);
	}
	
	public static String today() {
		long time = System.currentTimeMillis();
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		String date = df.format(new Date(time));
		
		return date;
	}
}
