package com.dio.warehousespring;

import java.util.Properties;

import org.springframework.beans.BeansException;
import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;

public class MyHandlerMapping extends SimpleUrlHandlerMapping {

	@Override
	public void setMappings(Properties mappings) {
		// TODO Auto-generated method stub
		super.setMappings(mappings);
	}

	@Override
	public void initApplicationContext() throws BeansException {
		// TODO Auto-generated method stub
		super.initApplicationContext();
	}

}