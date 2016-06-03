package com.bjbsh.heritrix.pm25.policy;

import org.apache.commons.lang.StringUtils;
import org.archive.crawler.frontier.SurtAuthorityQueueAssignmentPolicy;
import org.archive.net.UURI;

public class MultiURIQueueAssignmentPolicy extends SurtAuthorityQueueAssignmentPolicy {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int subPathCnt;
	
	protected String getCoreKey(UURI basis) {
		
//		System.out.println("=====> get key : " + basis);
		
        String candidate = getSurtAuthority(basis.getSurtForm(), new String(basis.getRawPath()));
        String key = candidate.replace(':','#').replace('/', ',');
        
//        System.out.println("=====> class key : " + key);
        
        return key;
    }
    
    protected String getSurtAuthority(String surt, String rawPath) {
    	
//    	surt = http://(com,pm25,www,)/city/mon/aqi/%e5%8d%97%e6%8a%95/%e5%9f%94%e9%87%8c.html
    	
    	int subPath = 0;
        int indexOfOpen = surt.indexOf("://(");
        int indexOfClose = surt.indexOf(")");
        if (indexOfOpen == -1 || indexOfClose == -1 || ((indexOfOpen + 4) >= indexOfClose)) {
            return DEFAULT_CLASS_KEY;
        }
        
        String result = surt.substring(indexOfOpen + 4, indexOfClose + subPath);
        
        if(getSubPathCnt() > 0 && StringUtils.isNotEmpty(rawPath)) {
        	int ch = 0;
        	for(int i = 0; i < rawPath.length(); i++) {
        		if(rawPath.charAt(i) == '/') {
        			ch++;
        			if(ch > getSubPathCnt()) {
        				subPath = i;
        				break;
        			}
        		}
        	}
        	
        	if(ch <= getSubPathCnt()) {
        		subPath = (rawPath.length());
        	}
        	
        	result = result + rawPath.substring(1, subPath);
        }
        
        return result;
    }

	public int getSubPathCnt() {
		return subPathCnt;
	}

	public void setSubPathCnt(int subPathCnt) {
		this.subPathCnt = subPathCnt;
	}

    
}
