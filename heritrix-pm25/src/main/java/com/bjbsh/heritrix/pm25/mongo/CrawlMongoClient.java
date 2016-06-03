package com.bjbsh.heritrix.pm25.mongo;

import java.io.Serializable;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class CrawlMongoClient implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String host;
	private int port;
	private String dbname;
	
	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	
//	public CrawlMongoClient(String host, int port, String dbname) {
//		this.host = host;
//		this.port = port;
//		this.dbname = dbname;
//	}
//	
//	public void init() {
//		
//	}
	
	public boolean connection() {
		
		if(null == mongoClient) {
			
			mongoClient = new MongoClient( getHost() , getPort() );
		       
	        // 连接到数据库
	        mongoDatabase = mongoClient.getDatabase(getDbname()); 
		}
		
		return mongoClient != null && mongoDatabase != null;
	}
	
	public void insertOne(String tbl, Document doc) {
		MongoCollection<Document> collection = mongoDatabase.getCollection(tbl);
        collection.insertOne(doc);
	}
	
	public void insert(String tbl, List<Document> docs) {
		MongoCollection<Document> collection = mongoDatabase.getCollection(tbl);
        collection.insertMany(docs);
	}
	
	public boolean hasRecode(String tbl, String key) {
		
		long cnt = mongoDatabase.getCollection(tbl).count(Filters.eq("key", key));
		
		return (cnt > 0);
	}
	
	public void close() {
		this.mongoClient.close();
		
		this.mongoClient = null;
		this.mongoDatabase = null;
		
	}
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}
	
	

	public static void main(String[] args) {
		
		MongoClient mongoClient = new MongoClient( "182.92.152.223" , 27017 );
	       
        // 连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("crawl_pm25"); 
        
        MongoCollection<Document> collection = mongoDatabase.getCollection("tbl_pm25");
        Document document = new Document("title", "MongoDB").  
                append("description", "database").  
                append("likes", 100).  
                append("by", "Fly");  
        
        collection.insertOne(document);
        
        FindIterable<Document> findIterable = collection.find();  
        MongoCursor<Document> mongoCursor = findIterable.iterator();  
        while(mongoCursor.hasNext()){  
           System.out.println(mongoCursor.next());  
        }  
        
        System.out.println("Connect to database successfully");
	}
}
