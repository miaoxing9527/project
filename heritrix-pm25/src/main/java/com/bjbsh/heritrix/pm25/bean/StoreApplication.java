package com.bjbsh.heritrix.pm25.bean;

import java.io.Serializable;

import org.archive.crawler.restlet.EngineApplication;

public class StoreApplication implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static EngineApplication application;

	public StoreApplication() {
		
		System.out.println("@@@@@@@@@@@@@@@@@@@@@ new StoreApplication @@@@@@@@@@@@@@@@@@@@@");
		if(null == application) {
			
			System.out.println(" application is null ");
			
			application = (EngineApplication) EngineApplication.getCurrent();
		}
		
	}
	
	public synchronized EngineApplication getApplication() {
		return application;
	}
	
	
}
