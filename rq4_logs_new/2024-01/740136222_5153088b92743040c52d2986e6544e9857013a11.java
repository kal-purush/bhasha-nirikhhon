package com.wefoundi.weblog.weblog.common.aspect;

import com.wefoundi.weblog.weblog.common.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Aspect
@Component
@Slf4j
public class ApiOperationLogAspect {

    /**
     * 以自定义 @ApiOperationLog 注解为切点，凡是添加 @ApiOperationLog 的方法，都会执行环绕中的代码
     */
    @Pointcut("@annotation(com.wefoundi.weblog.weblog.common.aspect.ApiOperationLog)")
    public void apiOperationLog() {
    }


    @Around("apiOperationLog()")
    public Object doAround(ProceedingJoinPoint joinPoint) throws Throwable {
        try {
            long startTime = System.currentTimeMillis();

            MDC.put("traceId", UUID.randomUUID().toString());

            String className = joinPoint.getTarget().getClass().getSimpleName();
            String methodName = joinPoint.getSignature().getName();
            Object[] args = joinPoint.getArgs();
            String argsJsonStr = Arrays.stream(args).map(toJsonStr()).collect(Collectors.joining(","));

            String description = getApiOperationLogDescription(joinPoint);
            Object result = joinPoint.proceed();
            long executionTime = System.currentTimeMillis() - startTime;

            log.info("====== 请求结束: [{}], 耗时: {}ms, 出参: {} ===============", description, executionTime, JsonUtil.toJsonString(result));
            return result;
        } finally {
            MDC.clear();
        }
    }

    private String getApiOperationLogDescription(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        ApiOperationLog apiOperationLog = method.getAnnotation(ApiOperationLog.class);
        return apiOperationLog.description();
    }

    private Function<Object, String> toJsonStr() {
        return arg -> JsonUtil.toJsonString(arg);
    }
}