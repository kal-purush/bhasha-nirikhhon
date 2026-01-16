/**********************************************************************************
 * KnightsMovesApplication.java
 *
 * @author Shrinivas Zilli (szilli@bekast.de)
 * Created on Mar 28, 2018
 *
 * Copyright (c) 2018 Bekast IT Consulting GmbH, All rights reserved.
 **********************************************************************************/

package com.szilli.cn.excercise;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

/**
 * The Class KnightsMovesApplication.
 */
@SpringBootApplication
public class KnightsMovesApplication extends SpringBootServletInitializer {

	/**
	 * Instantiates a new knights moves application.
	 */
	public KnightsMovesApplication() {
		super();
		setRegisterErrorPageFilter(false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.boot.web.servlet.support.SpringBootServletInitializer#
	 * configure(org.springframework.boot.builder.SpringApplicationBuilder)
	 */
	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(KnightsMovesApplication.class);
	}

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		SpringApplication.run(KnightsMovesApplication.class, args);
	}

}