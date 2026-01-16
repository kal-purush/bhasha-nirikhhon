package com.niki.account.aop;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.niki.account.handler.GlobalExceptionHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description :restcontroller日志拦截
 * @Author : yarm.yang
 * @Date : 2020/3/27 11:10
*/
@Aspect
@Component
@Slf4j
public class RestLogAspect {
    /**
     * @Description :切点
     * @Author : yarm.yang
     * @Date : 2020/3/27 11:26
     * @Return : 
    */
    @Pointcut("execution(public * com.niki.account.controller..*.*(..))")
    public void pointcut(){}

    @Around("pointcut()")
    public Object handle(ProceedingJoinPoint joinPoint) throws Throwable {
        // 目标方法返回值
        Object result = null;
        Long watchTime = 0L;
        StopWatch stopWatch = new StopWatch();
        try {
            HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
            //IP地址
            String ipAddr = getRemoteHost(request);
            String url = request.getRequestURL().toString();
            // url中的参数
            String urlParam = request.getQueryString();
            // 方法参数列表
            String methodParam = this.paramToJson(joinPoint);
            log.info("入参:请求源IP:{},请求URL:{},请求URL参数：{},切点方法参数Json列表:{}", ipAddr, url, urlParam, methodParam);

            // 执行目标方法
            stopWatch.start();
            result = joinPoint.proceed();
            stopWatch.stop();
            return result;
        }catch (Exception e){
            log.error("日志aop拦截Exception异常：{}", JSONObject.toJSON(e.getMessage()).toString());
//            result = GlobalExceptionHandler.handleExeption(e);
            return result;
        }catch (Throwable throwable){
            log.error("日志aop拦截Throwable异常：{}", JSONObject.toJSON(throwable.getMessage()).toString());
//            result = GlobalExceptionHandler.handleThrowable(throwable);
        }finally {
            String respParam = postHandle(result);
            watchTime = stopWatch.getTime();
            log.info("出参:{},请求时间：{}ms", respParam, watchTime);
        }
        return result;
    }



    /**
     * @Description :参数列表转json
     * @Author : yarm.yang
     * @Date : 2020/3/27 11:27
     * @Return : 
    */
    private String paramToJson(ProceedingJoinPoint joinPoint) {
        String reqParam = "";
        // 参数值
        Object[] args = joinPoint.getArgs();
        ParameterNameDiscoverer pnd = new DefaultParameterNameDiscoverer();
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        String[] parameterNames = pnd.getParameterNames(method);
        Map<String, Object> paramMap = new HashMap<>(32);
        for (int i = 0; i < parameterNames.length; i++) {
            paramMap.put(parameterNames[i], args[i]);
        }
        try {
            reqParam = JSONObject.toJSONString(paramMap);
        }catch (Exception e){
            reqParam = paramMap.toString();
        }
        return reqParam;
    }

    /**
     * @Description :返回数据
     * @Author : yarm.yang
     * @Date : 2020/3/27 11:27
     * @Return :
    */
    private String postHandle(Object retVal) {
        if(null == retVal){
            return "";
        }
        return JSON.toJSONString(retVal);
    }

    /**
     * @Description :获取目标主机的ip
     * @Author : yarm.yang
     * @Date : 2020/3/27 11:27
     * @Return :
    */
    private String getRemoteHost(HttpServletRequest request) {
        String ip = request.getHeader("x-forwarded-for");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return "0:0:0:0:0:0:0:1".equals(ip) ? "127.0.0.1" : ip;
    }
}