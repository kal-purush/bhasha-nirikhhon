package com.lucamusic.user.controller.error;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.DefaultErrorAttributes;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.WebRequest;

import com.lucamusic.user.controller.UserController;

@Component
public class CustomErrorAttributes extends DefaultErrorAttributes {
	private static final Logger logger = LoggerFactory.getLogger(UserController.class);
	private static final DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

	@Override
	public Map<String, Object> getErrorAttributes(WebRequest request, ErrorAttributeOptions options){
		logger.info("------- getErrorAttributes() " + options);
		
		Map<String, Object> errorAttributes = super.getErrorAttributes(request, options);
		logger.info("------- getErrorAttributes() " + options);
		Object timestamp = errorAttributes.get("timestamp");
		if(timestamp == null) {
			errorAttributes.put("timestamp", dateFormat.format(new Date()));
		} else {
			errorAttributes.put("timestamp", dateFormat.format((Date) timestamp));
		}
		
		errorAttributes.put("jdk", System.getProperty("java.version"));
		
		return errorAttributes;
		
	}

}