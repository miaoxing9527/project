package com.bjbsh.heritrix.pm25.log;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.archive.crawler.framework.Engine;
import org.archive.crawler.io.StatisticsLogFormatter;
import org.archive.crawler.reporting.AlertThreadGroup;
import org.archive.io.GenerationFileHandler;
import org.archive.spring.ConfigPath;
import org.archive.util.ArchiveUtils;
import org.archive.util.FileUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.Lifecycle;

public class MongoLoggerModule implements Lifecycle, InitializingBean, DisposableBean {

	private static final long serialVersionUID = 1L;

    protected ConfigPath path = new ConfigPath(Engine.LOGS_DIR_NAME,"${launchId}/logs"); 
    
 // key log names
    private static final String LOGNAME_MONGODB = "mongodb";  
    
    public ConfigPath getPath() {
        return path;
    }
    public void setPath(ConfigPath cp) {
        this.path.merge(cp);
    }

	protected ConfigPath mongoDbLogPath = new ConfigPath("mongodb.log", "mongodb.log");
	
    public ConfigPath getMongoDbLogPath() {
		return mongoDbLogPath;
	}
	public void setMongoDbLogPath(ConfigPath mongoDbLogPath) {
		this.mongoDbLogPath = mongoDbLogPath;
	}

	private transient Logger mongodb;
    
    public Logger getMongodb() {
		return mongodb;
	}
	public void setMongodb(Logger mongodb) {
		this.mongodb = mongodb;
	}

	boolean isRunning = false; 
    private transient AlertThreadGroup atg;
    
	@Override
	public boolean isRunning() {
		return isRunning;
	}

	@Override
	public void start() {
		if(isRunning) {
            return; 
        }
        this.atg = AlertThreadGroup.current();
        try {
            FileUtils.ensureWriteableDirectory(getPath().getFile());
            setupLogs();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        isRunning = true; 
		
	}
	
	private void setupLogs() throws IOException {
        String logsPath = getPath().getFile().getAbsolutePath() + File.separatorChar;
        mongodb = Logger.getLogger(LOGNAME_MONGODB + "." + logsPath);
        
        setupLogFile(mongodb, getMongoDbLogPath().getFile().getAbsolutePath(), new MongoDbLogFormatter(), true);
	}

	GenerationFileHandler fh;
	private void setupLogFile(Logger logger, String filename, Formatter f, boolean shouldManifest) throws IOException, SecurityException {
        logger.setLevel(Level.INFO); // set all standard loggers to INFO
        fh = GenerationFileHandler.makeNew(filename, false, shouldManifest);
        fh.setFormatter(f);
        logger.addHandler(fh);
//        addToManifest(filename, 'L', shouldManifest);
        logger.setUseParentHandlers(false);
//        this.fileHandlers.put(logger, fh);
    }
	
	@Override
	public void stop() {
		isRunning = false; 	
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if(mongoDbLogPath.getBase()==null) {
			mongoDbLogPath.setBase(getPath());
        }
	}

	@Override
	public void destroy() throws Exception {
		closeLogFiles();
	}

	public void closeLogFiles() {
        fh.close();
        mongodb.removeHandler(fh);
    }
	
	class MongoDbLogFormatter extends Formatter {

		@Override
		public String format(LogRecord record) {

			StringBuffer buf = new StringBuffer();
			
			long time = System.currentTimeMillis();
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			String date = df.format(new Date(time));
		
			buf.append(date);
			buf.append(" ");
			buf.append(record.getMessage());
			buf.append("\n");
			return new String(buf);
		}	
	}
	
	public static void main(String[] args) {
	
		long time = System.currentTimeMillis();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//        df.setTimeZone(TimeZone.getTimeZone("UTC"));
		String s = df.format(new Date(time));
		
		System.out.println(s);
		
	}
}
