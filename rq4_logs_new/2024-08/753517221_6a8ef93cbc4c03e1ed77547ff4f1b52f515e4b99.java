package com.itcook.cooking.api.global.aop;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Aspect
@Slf4j
@Component
public class LogAspect {

    @Pointcut("execution(* com.itcook.cooking..*Controller.*(..))")
    public void controller() {
    }

    @Around("controller()")
    public Object loggingBefore(ProceedingJoinPoint joinPoint) throws Throwable {
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes())
            .getRequest();
        String controllerName = joinPoint.getSignature().getDeclaringType().getName();
        String methodName = joinPoint.getSignature().getName();
        String uri = URLDecoder.decode(request.getRequestURI(), "UTF-8");

        log.info("[{}] {}", request.getMethod(), uri);
        log.info("method: {}.{}", controllerName ,methodName);
        log.info("query params: {}", getParams(request));
        Object[] args = joinPoint.getArgs();
        if (args.length == 0) log.info("no req body");
        for (Object arg : args) {
            log.info("binding request body = {}", arg);
        }


        return joinPoint.proceed();
    }

    private Object getParams(HttpServletRequest request) {
        Map<Object, Object> paramMap = new HashMap<>();
        Enumeration<String> params = request.getParameterNames(); // 쿼리 파라미터를 가져옴

        while (params.hasMoreElements()) {
            String param = params.nextElement();
            String replaceParam = param.replaceAll("\\.", "-");
            paramMap.put(replaceParam, request.getParameter(param));
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(paramMap);
        } catch (JsonProcessingException e) {
            log.error("JSON 파싱 에러", e);
            return "{}";
        }
    }



}