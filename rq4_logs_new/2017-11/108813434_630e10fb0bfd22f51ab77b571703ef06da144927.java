package com.zcc.login.common.aspect;

import java.util.Locale;

import javax.servlet.http.HttpServletRequest;

import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.LocaleResolver;

import com.zcc.login.common.constant.CommonConstant;
import com.zcc.login.common.exception.CommonException;
import com.zcc.login.common.exception.ServiceException;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ZhangChicheng on 2017/11/22.
 */
@Aspect
@CommonsLog
@Component
@Order(AspectOrder.MEDIUM_PRIORITY)
public class CommonExceptionI18nAspect {
	@Autowired
	private MessageSource messageSource;
	@Autowired
	private LocaleResolver localeResolver;

	@AfterThrowing(pointcut = "execution(* com.zcc.login.service.impl.*Impl.*(..))", throwing = "ex")
	public void handleExceptionMsg(CommonException ex) {
		log.debug("*********CommonExceptionI18nAspect.handleExceptionMsg start");
		if (ex == null) {
			log.debug("*********** params ex is null");
			return;
		}
		int errorCode = ex.getErrorCode();
		String errorMsg = ex.getErrorMsg();
		log.debug("*******errorCode : " + errorCode);
		log.debug("*******errorMsg : " + errorMsg);

		HttpServletRequest request = ((ServletRequestAttributes)RequestContextHolder.getRequestAttributes()).getRequest();

		Locale locale = localeResolver.resolveLocale(request);

		String propertyCode;
		String message;
		if (ex instanceof ServiceException) {
			propertyCode = CommonConstant.SERVICE_EX_PREFIX + errorCode;
		} else {
			propertyCode = CommonConstant.COMMON_EX_PREFIX + errorCode;
		}
		//Todo args
		try {
			message = messageSource.getMessage(propertyCode, null, locale);
		} catch (Exception e) {
			log.info("*******messageSource.getMessage cannot got msg" + propertyCode);
			propertyCode = CommonConstant.COMMON_EX_PREFIX + CommonException.COMMON_DEFAULT_CODE;
			message = messageSource.getMessage(propertyCode, null, locale);
		}
		ex.setErrorMsg(message);
		log.debug("**********CommonException.errorMsg is : " + ex.getErrorMsg());
		log.debug("*********CommonExceptionI18nAspect.handleExceptionMsg end");
	}
}