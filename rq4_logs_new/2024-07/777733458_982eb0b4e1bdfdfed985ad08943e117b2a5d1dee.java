package com.practice.practiceProject.aspect;

import lombok.extern.log4j.Log4j2;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Log4j2
public class ExecutionTimeTrackerAspect {

    @Around("@annotation(com.practice.practiceProject.annotation.TrackExecutionTime)")
    public Object trackExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        final long startTime = System.currentTimeMillis();
        log.info("Started API for class {}, for method {}, (start time {} ms)", joinPoint.getSignature().getDeclaringTypeName(), joinPoint.getSignature().getName(), ( startTime));
        Object proceed = joinPoint.proceed();
        final long endTime = System.currentTimeMillis();
        log.info("Completed API for class {}, for method {}, (total time {} ms)", joinPoint.getSignature().getDeclaringTypeName(), joinPoint.getSignature().getName(), (endTime - startTime));
        return proceed;
    }
}