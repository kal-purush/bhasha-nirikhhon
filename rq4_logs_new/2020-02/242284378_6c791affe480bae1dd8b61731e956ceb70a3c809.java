package com.zjb.aop.aop;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.context.annotation.Configuration;

/**
 * 处理service
 * @author zhaojianbo
 * @date 2020/2/23 11:56
 */
@Aspect
@Configuration
public class AopAnonConfig {
    @Pointcut(value = "@annotation(com.zjb.aop.anon.WebLog)")
    public void pointCut() {
    }

    @Before("pointCut()")
    public void doBefore(JoinPoint joinPoint) {
        System.out.println("anno before.........");
    }

    @AfterReturning(pointcut = "pointCut()", returning = "ret")
    public void doAfterRetrning(JoinPoint joinPoint, Object ret) {
        String name = joinPoint.getSignature().getName();
        System.out.println("注解返回" + "------" + name + "----" + ret);
    }
}