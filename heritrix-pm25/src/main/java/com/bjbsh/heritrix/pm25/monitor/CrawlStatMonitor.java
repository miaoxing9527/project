package com.bjbsh.heritrix.pm25.monitor;

import java.util.logging.Logger;

import org.archive.crawler.event.CrawlStateEvent;
import org.archive.crawler.framework.CrawlController;
import org.archive.crawler.framework.CrawlController.State;
import org.archive.crawler.framework.CrawlController.StopCompleteEvent;
import org.archive.crawler.framework.CrawlJob;
import org.archive.modules.Processor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.bjbsh.heritrix.pm25.bean.StoreApplication;
import com.bjbsh.heritrix.pm25.writer.MongoWriter;

public class CrawlStatMonitor implements ApplicationListener {

	private final static Logger LOGGER = Logger.getLogger(CrawlStatMonitor.class.getName());

	@Autowired
	private Processor warcWriter;
	
	@Autowired
	private StoreApplication storeApplication;
	
	
	private int seconds;
	
	CrawlController controller;
	CrawlJob crawlJob;
	
	public CrawlStatMonitor() {
	}
	
	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		
		String job = controller.getMetadata().getJobName();
		
		if(event instanceof CrawlStateEvent) {
			
            CrawlStateEvent event1 = (CrawlStateEvent)event;
            
            LOGGER.info("[ ApplicationEvent ]" + event1.getState());
            
            if(event1.getState() == State.FINISHED) {
            	
				MongoWriter mongoWriter = (MongoWriter)warcWriter;
				if(!mongoWriter.getData().isEmpty()) {
					
					LOGGER.info("[CrawlStatMonitor] push mongo data start");
					
					mongoWriter.push();
					mongoWriter.clear();
					
					LOGGER.info("[CrawlStatMonitor] push mongo data finished");
				}
				else {
					LOGGER.info("[CrawlStatMonitor] no data push for mongo");
				}
            	
            	if(null == storeApplication.getApplication()) {
					
            		LOGGER.info("[CrawlStatMonitor] application is null !!!!" );
					return ;
				}

            	crawlJob = storeApplication.getApplication().getEngine().getJob(job);
            	crawlJob.teardown();
            }
        }
		else {
			
			if(event instanceof StopCompleteEvent) {
				
				try {
					Thread.sleep(1000 * seconds);
				} catch (InterruptedException e) {
					// do nothing
				}
				
				if(null == storeApplication.getApplication()) {
					
					LOGGER.info("[CrawlStatMonitor] application is null !!!!" );
					
					return ;
				}
				
	            crawlJob = storeApplication.getApplication().getEngine().getJob(job);
	            
	            LOGGER.info("[CrawlStatMonitor] jb is null : " + ( crawlJob == null ) + " | is launchable :" + ( crawlJob.isLaunchable() ));
					
				if(crawlJob != null && crawlJob.isLaunchable()) {
					crawlJob.launch();
					
					LOGGER.info("[CrawlStatMonitor] @@@@@@@@@@@@@@@@ restart job " + job + " success @@@@@@@@@@@@@@@@" );
				}
				else {
					LOGGER.info("[CrawlStatMonitor] @@@@@@@@@@@@@@@@ restart job " + job + " failed  @@@@@@@@@@@@@@@@" );
				}
			}
			
		}
	}
	
	/** Autowire access to CrawlController **/
    @Autowired
    public void setCrawlController(CrawlController controller) {
        this.controller = controller;
    }
    public CrawlController getCrawlController() {
        return this.controller;
    }

	public int getSeconds() {
		return seconds;
	}

	public void setSeconds(int seconds) {
		this.seconds = seconds;
	}
}
