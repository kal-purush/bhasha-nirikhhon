package com.wiggin.springboot.demo.aspect;

import org.apache.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

import javax.servlet.http.HttpServletRequest;

@Aspect
@Component
public class ControllerAspect {

    private Logger logger = Logger.getLogger(getClass());

    @Pointcut("execution(public * com.wiggin.springboot.demo.controller.*.*(..))")
    public void log(){

    }

    @Before("log() && this(proxy)")
    public void beforeAdvide(JoinPoint point, Object proxy){
        Signature signature = point.getSignature();
        RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = (HttpServletRequest) requestAttributes.resolveReference(RequestAttributes.REFERENCE_REQUEST);
        logger.info("测试切面"+signature.getDeclaringTypeName()+request.getParameter("id"));
//处理逻辑
    }
}