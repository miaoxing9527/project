package com.bjbsh.heritrix.pm25.monitor;

import java.util.List;
import java.util.logging.Logger;

import org.archive.crawler.event.CrawlStateEvent;
import org.archive.crawler.framework.CrawlController;
import org.archive.crawler.framework.CrawlController.State;
import org.archive.crawler.framework.CrawlController.StopCompleteEvent;
import org.archive.crawler.framework.CrawlJob;
import org.archive.modules.Processor;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.bjbsh.heritrix.pm25.bean.StoreApplication;
import com.bjbsh.heritrix.pm25.mongo.CrawlMongoClient;
import com.bjbsh.heritrix.pm25.weather.WeatherParser;
import com.bjbsh.heritrix.pm25.writer.MongoWriter;

public class WeatherMonitor implements ApplicationListener {

	private final static Logger LOGGER = Logger.getLogger(WeatherMonitor.class.getName());
	
	@Autowired
	private CrawlMongoClient mongoClient;
	
	public WeatherMonitor() {
	}
	
	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		
		if(event instanceof CrawlStateEvent) {
			
            CrawlStateEvent event1 = (CrawlStateEvent)event;
            
            if(event1.getState() == State.RUNNING) {

				LOGGER.info("[WeatherMonitor] push weather mongo data start");
				
				mongoClient.connection();
				
				// 1, check today data is exist in db
				String today = WeatherParser.today();
				if(mongoClient.hasRecode("tbl_weather", today)) {
					mongoClient.close();
					return ;
				}
					
				// 2, insert
				List<Document> dataList = WeatherParser.parser();

				for(Document doc : dataList) {
					
					mongoClient.insertOne("tbl_weather", doc);
					
				}
				
				mongoClient.close();
				
				LOGGER.info("[WeatherMonitor] push weather mongo data finished");
				
            }
        }
	}

}
