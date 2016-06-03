package com.bjbsh.heritrix.pm25.writer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang.StringUtils;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.util.ArchiveUtils;
import org.archive.util.TextUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;

import com.bjbsh.heritrix.pm25.log.MongoLoggerModule;
import com.bjbsh.heritrix.pm25.mongo.CrawlMongoClient;

public class MongoWriter extends Processor{

	private final static Logger LOGGER = Logger.getLogger(MongoWriter.class.getName());
	
	private static final String PATTERN_CRAWL_URL = ".*/city/mon/(aqi|pm2_5|pm10|so2|no2|co|o3)/.*/.*(html)$";
	private static final String TABLE = "tbl_pm25";
	
	private HashMap<String, Document> data = new HashMap<String, Document>();
//	private String mongodb_host;
//	private String mongodb_port;
//	private String mongodb_database;
	
	@Autowired
	private MongoLoggerModule mongoLoggerModule;
	
	@Autowired
	private CrawlMongoClient mongoClient;
	
//	http://www.pm25.com/city/kuerle.html
//	http://www.pm25.com/city/mon/aqi/%E5%BA%93%E5%B0%94%E5%8B%92/%E5%AD%94%E9%9B%80%E5%85%AC%E5%9B%AD.html

	@Override
	protected boolean shouldProcess(CrawlURI uri) {
		
		// 1, status success check
		int status = uri.getFetchStatus();
		if(HttpStatus.SC_OK != status) {
			return false;
		}
		
		// 2, URL check
		if(matche(uri)) {
			return true;
		}

		return false;
	}
	
	@Override
	protected void innerProcess(CrawlURI uri) throws InterruptedException {

		System.out.println("===> process uri:" + decode(uri.toString()) + " #" + uri.getThreadNumber());
		
		String crawlUri = uri.toString();
		Type type = Type.getType(crawlUri);
		if(Type.NONE == type) {
			return ;
		}
		
		LOGGER.info("process uri   : " + decode(crawlUri));
		
		String column = type.pattern.replace("/", "");
		
		String[] strs = crawlUri.replace(".html", "").split("/");
    	int len = strs.length;
    	String city = strs[len-2];
    	String station = strs[len-1];
    	
    	String prefix = uri.getRecorder().getContentReplayPrefixString(1000);
		Charset charset = getContentDeclaredCharset(uri, prefix);
		if(null == charset) {
			charset = Charset.forName("utf-8");
		}
		String content = null;
		try {
			content = uri.getRecorder().getContentReplayCharSequence(charset).toString();
		} catch (IOException e) {
			e.printStackTrace();	
			return;
		}
		
		content = content.replaceAll("\\s+", "");
		
		String timer24h = get24hTime(content);
		String data24h = "";
		
		if(Type.AQI == type) {
			data24h = get24hDataAQI(content);
		}
		else {
			data24h = get24hData(content);
		}
		
		if(StringUtils.isEmpty(timer24h) || StringUtils.isEmpty(data24h)) {
			return ;
		}
		
//    	System.out.println("===> city:" + decode(city));
//    	System.out.println("===> station:" + decode(station));
//    	System.out.println("===> tbl:" + column);
//    	System.out.println("===> timer24h:" + timer24h);
//    	System.out.println("===> data24h:" + data24h);
    	
		LOGGER.info("process timer : " + timer24h);
		LOGGER.info("process data  : " + data24h);
		
		mongoLoggerModule.getMongodb().info("#" + uri.getThreadNumber() + " {" + "city:" + decode(city) + ", station:" + decode(station) + ", type:" + column + ", time:[" + timer24h + "], data:[" + data24h + "]}");
		
    	String[] timer_ary = timer24h.split(",");
    	String[] data_ary = data24h.split(",");
    	if(24 != timer_ary.length || 24 != data_ary.length) {
    		return ;
    	}
    	
    	for(int i = 0; i < timer_ary.length; i++) {
    		Document doc;
    		
    		if(StringUtils.isEmpty(timer_ary[i]) || timer_ary[i].length() != 18) {
    			return ;
    		}
    		
    		String time = convertTime(timer_ary[i]);
    		
    		
    		String key = time.concat(city).concat(station);
    		if(data.containsKey(key)) {
    			doc = data.get(key);
    		}
    		else {
    			doc = new Document();
    		}
    		
    		doc.append("key", key);
    		doc.append("day", time.substring(0, 8));
    		doc.append("hours", time.substring(8));
    		doc.append("time", time);
    		doc.append("city", decode(city));
    		doc.append("station", decode(station));
    		doc.append(column, data_ary[i]);
    		
    		data.put(key, doc);
    		
//    		LOGGER.info(doc.toJson());
    	}
    	
    	
	}
	
	public synchronized void push() {
		
//		String host = getMongodb_host();
//		int port = Integer.valueOf(getMongodb_port());
//		String dbname = getMongodb_database();
//		if(StringUtils.isEmpty(host)) {
//			return ;
//		}
//		if(0 == port) {
//			port = 27017;
//		}
//		if(StringUtils.isEmpty(dbname)) {
//			return ;
//		}		
//
//		LOGGER.info("[MongoDb] connect to : " + host + ":" + port + "@" + dbname);
		
//		CrawlMongoClient mongoClient = new CrawlMongoClient(host, port, dbname);
		mongoClient.connection();
		
		List<Document> list = new ArrayList<Document>();
		int limit = 50;
		
		for(String key : data.keySet()) {
			
			if(mongoClient.hasRecode(TABLE, key)) {
				continue ;
			}
			
//			mongoClient.insertOne(TABLE, data.get(key));
			
			list.add(data.get(key));
			if(list.size() == limit) {
				mongoClient.insert(TABLE, list);
				list.clear();
			}
		}
		
		if(!list.isEmpty()) {
			mongoClient.insert(TABLE, list);
			list.clear();
		}
		
		mongoClient.close();
		
		LOGGER.info("[MongoDb] process recode count : " + data.keySet().size());
	}
	
	public String convertTime(String from) {
		
//		"19\u65e511\u65f6"
		
		int day = Integer.valueOf(from.substring(1, 3));
		int hour = Integer.valueOf(from.substring(9, 11));
		
		Calendar now = Calendar.getInstance(); 

		int now_year = now.get(Calendar.YEAR);
		int now_month = now.get(Calendar.MONTH) + 1;
		int now_day = now.get(Calendar.DAY_OF_MONTH);
//		int now_hour = now.get(Calendar.HOUR_OF_DAY);
		
//		int now_year = 2016;
//		int now_month = 2;
//		int now_day = 10;
//		int now_hour = 10;
		
//		System.out.println(now_year + ":" + now_month + ":" + now_day + ":" + now_hour);
		
		StringBuffer buffer = new StringBuffer();
		if(now_month == 1 && now_day == 1 && day > 1) {
			buffer.append(now_year - 1);
			buffer.append("12");
		}
		else {
			buffer.append(now_year);
			int month = now_month;
			if(now_day == 1 && day > 1) {
				month = now_month - 1;
			}
			if(month < 10) {
				buffer.append("0");
			}
			buffer.append(month);
		}
		
		if(day < 10) {
			buffer.append("0");
		}
		buffer.append(day);
		
		if(hour < 10) {
			buffer.append("0");
		}
		buffer.append(hour);
		
		return new String(buffer);
	}	

	public String decode( String str) {
		URLCodec codec = new URLCodec();
		String result = "";
		
		try {
			result = codec.decode(str.toString(), "utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DecoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	// 1. look for <meta http-equiv="content-type"...>
    // 2. if not found then look for <meta charset="">
    // 3. if not found then <?xml encoding=""...?>
    protected Charset getContentDeclaredCharset(CrawlURI curi, String contentPrefix) {
        String charsetName = null; 
        // <meta http-equiv="content-type" content="text/html; charset=iso-8859-1">
        Matcher matcher = TextUtils.getMatcher("(?is)<meta\\s+[^>]*http-equiv\\s*=\\s*['\"]content-type['\"][^>]*>", contentPrefix);
        if (matcher.find()) {
            String metaContentType = matcher.group();
            TextUtils.recycleMatcher(matcher); 
            matcher = TextUtils.getMatcher("charset=([^'\";\\s>]+)", metaContentType);
            if (matcher.find()) {
                charsetName = matcher.group(1); 
            }
            TextUtils.recycleMatcher(matcher); 
        }

        if(charsetName==null) {
            // <meta charset="utf-8">
            matcher = TextUtils.getMatcher("(?si)<meta\\s+[^>]*charset=['\"]([^'\";\\s>]+)['\"]", contentPrefix);
            if (matcher.find()) {
                charsetName = matcher.group(1); 
                TextUtils.recycleMatcher(matcher); 
            } else {
                // <?xml version="1.0" encoding="utf-8"?>
                matcher = TextUtils.getMatcher("(?is)<\\?xml\\s+[^>]*encoding=['\"]([^'\"]+)['\"]", contentPrefix);
                if (matcher.find()) {
                    charsetName = matcher.group(1); 
                } else {
                    return null; // none found
                }
                TextUtils.recycleMatcher(matcher); 
            }
        }
        try {
            return Charset.forName(charsetName); 
        } catch (IllegalArgumentException iae) {
            curi.getAnnotations().add("unsatisfiableCharsetInHTML:"+charsetName);
            return null; 
        } 
    }
    
	protected boolean matche(CrawlURI uri) {
        Pattern p = Pattern.compile(PATTERN_CRAWL_URL);
        return p.matcher(getString(uri)).matches();
    }
    
    protected String getString(CrawlURI uri) {
        return uri.toString();
    }

    public static String get24hTime(String content) {
    	
    	String result = "";
    	Matcher matcher = TextUtils.getMatcher("(option_h24).+?((xAxis)(.+?));", content);
		if (matcher.find()) {
		
			String find = matcher.group(2);
//			System.out.println("matcher:" + find);
			
			Matcher dataMatcher = TextUtils.getMatcher("(data:\\[)(.+?)(\\].+)", find);
//			if (dataMatcher.find()){	
//				for(int i = 0; i < dataMatcher.groupCount(); i++) {
//					System.out.println("for data:" + dataMatcher.group(i));
//				}
//			}
			if (dataMatcher.find() && dataMatcher.groupCount() == 3) {
				result = dataMatcher.group(2);
//				System.out.println("data:" + result);
			}
		}
		return result;
    }

    public static String get24hData(String content) {
    	
    	String result = "";
    	Matcher matcher = TextUtils.getMatcher("(option_h24).+?((series)(.+?));", content);
		if (matcher.find()) {
		
			String find = matcher.group(2);
//			System.out.println("matcher:" + find);
			
			Matcher dataMatcher = TextUtils.getMatcher("(data:\\[)(.+?)(\\].+)", find);
			if (dataMatcher.find() && dataMatcher.groupCount() == 3) {
				result = dataMatcher.group(2);
//				System.out.println("data:" + result);
			}
		}
		return result;
    }
    
    public static String get24hDataAQI(String content) {
    	
    	String result = "";
    	Matcher matcher = TextUtils.getMatcher("(option_h24).+?((series)(.+?));", content);
		if (matcher.find()) {
		
			String find = matcher.group(2);
//			System.out.println("matcher:" + find);
			
			Matcher dataMatcher = TextUtils.getMatcher("(}.+)(data:\\[)(.+?)(\\].+)", find);
			if (dataMatcher.find() && dataMatcher.groupCount() > 3) {
				result = dataMatcher.group(3);
//				System.out.println("data:" + result);
			}
		}
		return result;
    }
    public static void main(String args[]) {
//    	
//    	String str = "123 var option_h24=series : [{ name:'������׼',type:'line',data:[354,354,347,347,500,500,500,500,500,500,185,500,178,142,154,150,144,140,163,163,175,175,197,197]          },{ name:'�й���׼',type:'line',data:[368,368,354,354,500,500,500,500,500,500,185,500,179,137,142,130,129,132,164,164,175,175,198,198]}]};series:456;";
//    	String timetest = "var option_h24 matcher,xAxis:[{type:'category',boundaryGap:false,data:[\"15\u65e519\u65f6\",\"15\u65e519\u65f6\",\"15\u65e520\u65f6\",\"15\u65e520\u65f6\",\"15\u65e521\u65f6\",\"15\u65e521\u65f6\",\"15\u65e522\u65f6\",\"15\u65e522\u65f6\",\"15\u65e523\u65f6\",\"15\u65e523\u65f6\",\"16\u65e500\u65f6\",\"16\u65e501\u65f6\",\"16\u65e502\u65f6\",\"16\u65e503\u65f6\",\"16\u65e504\u65f6\",\"16\u65e505\u65f6\",\"16\u65e506\u65f6\",\"16\u65e507\u65f6\",\"16\u65e508\u65f6\",\"16\u65e508\u65f6\",\"16\u65e509\u65f6\",\"16\u65e509\u65f6\",\"16\u65e510\u65f6\",\"16\u65e510\u65f6\"],axisLine:{lineStyle:{color:'#d4d4d4'}}}],yAxis:[{type:'value',axisLabel:{formatter:'{value}'},axisLine:{lineStyle:{color:'#d4d4d4'}}}],series:[{name:'������׼',type:'line',data:[354,354,347,347,500,500,500,500,500,500,185,500,178,142,154,150,144,140,163,163,175,175,197,197]},{name:'�й���׼',type:'line',data:[368,368,354,354,500,500,500,500,500,500,185,500,179,137,142,130,129,132,164,164,175,175,198,198]}]};";
//  	
//    	get24hTime(timetest);
//    	get24hData(str);
//    	
//    	String uri = "http://pm25.com/city/mon/pm2_5/����/��׼.html";
//    	
//    	Type type = Type.getType(uri);
//    	System.out.println(type == Type.PM25);
//    	System.out.println(type.pattern);
//    	
//    	String[] strs = uri.replace(".html", "").split("/");
//    	int len = strs.length;
//    	System.out.println(strs[len-2] + strs[len-1]);
//    	
//    	String test = "19\u65e500\u65f6";
//    	try {
//			System.out.println(test);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    }
    
    public void clear() {
    	this.data.clear();
    }
    
	public HashMap<String, Document> getData() {
		return data;
	}
//	public String getMongodb_host() {
//		return mongodb_host;
//	}
//	public void setMongodb_host(String mongodb_host) {
//		this.mongodb_host = mongodb_host;
//	}
//	public String getMongodb_port() {
//		return mongodb_port;
//	}
//	public void setMongodb_port(String mongodb_port) {
//		this.mongodb_port = mongodb_port;
//	}
//	public String getMongodb_database() {
//		return mongodb_database;
//	}
//	public void setMongodb_database(String mongodb_database) {
//		this.mongodb_database = mongodb_database;
//	}


	public static enum Type { 
        
        AQI("/aqi"),                  
        PM25("/pm2_5"),
        PM10("/pm10"),     
        SO2("/so2"),         
        NO2("/no2"), 
        CO("/co"),
        O3("/o3"),
        NONE(null);
        
        final private String pattern;
        
        Type(String regex) {
            if (regex == null) {
                pattern = null;
            } else {
                pattern = regex;
            }
        }
        
        public static Type getType(String uri) {
        	
        	if(StringUtils.isEmpty(uri)) return NONE;
        	
        	if(uri.contains(AQI.pattern)) {
        		return AQI;
        	}
        	else if(uri.contains(PM25.pattern)) {
        		return PM25;
        	}
        	else if(uri.contains(PM10.pattern)) {
        		return PM10;
        	}
        	else if(uri.contains(SO2.pattern)) {
        		return SO2;
        	}
        	else if(uri.contains(NO2.pattern)) {
        		return NO2;
        	}
        	else if(uri.contains(CO.pattern)) {
        		return CO;
        	}
        	else if(uri.contains(O3.pattern)) {
        		return O3;
        	}
        	
            return NONE;
        }
    }
}
