package com.zcc.login.common.aspect;

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ResponseBody;

import com.zcc.login.common.utils.PropertyMessageUtils;
import com.zcc.login.vo.CommonResponse;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ZhangChicheng on 2017/11/28.
 */
@Aspect
@Component
@CommonsLog
public class CommonResponseHandleAspect {
	private static final String MSG_PREFIX = "common.message.";

	@Around("execution(* com.zcc.login.controller.*.*(..))")
	public Object handleResponse(ProceedingJoinPoint joinPoint) throws Throwable {
		MethodSignature methodSignature = (MethodSignature)joinPoint.getSignature();
		Method method = methodSignature.getMethod();
		ResponseBody anno = method.getAnnotation(ResponseBody.class);
		if (anno != null) {
			long startTime = System.currentTimeMillis();
			CommonResponse returnValue = (CommonResponse)joinPoint.proceed();
			long endTime = System.currentTimeMillis();
			returnValue.setProcTime((endTime - startTime) + "ms");
			int returnCode = returnValue.getReturnCode();
			String propertyCode = MSG_PREFIX + returnCode;
			String message = PropertyMessageUtils.getMessage(propertyCode);
			returnValue.setReturnMsg(message);
			return returnValue;
		}
		return joinPoint.proceed();
	}
}