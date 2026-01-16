package com.github.rshtishi.recipe.controller;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.Order;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;

import ch.qos.logback.classic.Logger;

@Order(Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice
public class RecipeExceptionHandler {
	
	public static final String DEFAULT_ERROR_VIEW = "error";


	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(RecipeExceptionHandler.class);

	@ExceptionHandler(value = Exception.class)
	public ModelAndView defaultExceptionHandler(HttpServletRequest req, Exception e) throws Exception {

		if (AnnotationUtils.findAnnotation(e.getClass(), ResponseStatus.class) != null) {
			throw e;
		}
		ModelAndView mav = new ModelAndView();
		mav.setViewName(DEFAULT_ERROR_VIEW);
		return mav;
	}

}