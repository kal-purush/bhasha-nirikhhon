package com.zcc.login.common.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import com.zcc.login.common.annotation.DataSourceType;
import com.zcc.login.common.constant.DataSourceEnum;
import com.zcc.login.common.contextholder.DataSourceContextHolder;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ZhangChicheng on 2017/11/30.
 */
@Aspect
@Component
@CommonsLog
public class DataSourceAspect {

	@Around("execution(* com.zcc.login.service.impl.*Impl.*(..))")
	public Object switchDataSource(ProceedingJoinPoint joinPoint) throws Throwable {

		log.debug("Enter swichDatasource Aspect~~~~~~~~~~~~~~~~~~~~");
		MethodSignature methodSignature = (MethodSignature)joinPoint.getSignature();

		DataSourceType anno = methodSignature.getMethod().getAnnotation(DataSourceType.class);

		if (anno == null) {
			anno = (DataSourceType)methodSignature.getDeclaringType().getAnnotation(DataSourceType.class);
		}

		if (anno == null) {
			return joinPoint.proceed();
		}

		DataSourceEnum dataSourceEnum = anno.value();
		DataSourceContextHolder.setDataSourceType(dataSourceEnum);
		Object result;
		log.debug("Now datasource is : " + dataSourceEnum.name().toString());
		try {
			result = joinPoint.proceed();
		} catch (Throwable throwable) {
			throw throwable;
		} finally {
			DataSourceContextHolder.resetDataSourceType();
		}
		log.debug("Outer swichDatasource Aspect~~~~~~~~~~~~~~~~~~~~");
		return result;
	}
}