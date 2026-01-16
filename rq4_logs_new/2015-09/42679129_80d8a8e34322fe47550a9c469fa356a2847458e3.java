package com.leisucn.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WebUtils {
	
	private Logger logger = LogManager.getLogger(getClass());
	
	/**
	 * 转发
	 * @param target
	 * @param os
	 * @param method
	 */
	public void forward(String target, final OutputStream os, String method){
		
		if( logger.isDebugEnabled() ){
			logger.debug("forward to " + target);
		}
		
		if( StringUtils.isEmpty(target) ){
			logger.warn("target is empty!");
			return;
		}
		
		if( os == null ){
			logger.warn("output stream is null");
			return;
		}
		
		if( StringUtils.isEmpty(method) ){
			method = "GET";
		}
		
		try( CloseableHttpClient client = HttpClients.createDefault() ){
			
			HttpUriRequest request = method.equalsIgnoreCase("GET") ?
					new HttpGet(target) : new HttpPost(target);
			
			client.execute(request, new ResponseHandler<StatusLine>() {

				@Override
				public StatusLine handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
					StatusLine status = response.getStatusLine();
					HttpEntity entity = response.getEntity();
					try( InputStream is = entity.getContent() ){
						int length = -1;
						byte[] buffer = new byte[512*1000];
						while( (length = is.read(buffer)) != -1 ){
							os.write(buffer, 0, length);
						}
					}
					return status;
				}
			});
			
		} catch (IOException e) {
			logger.error(e);
		}
		
		
	}

}