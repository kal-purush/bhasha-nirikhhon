package by.tms.lesson48.configiration;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

import java.time.LocalTime;

@Aspect
@Component
@Slf4j
public class LoggingAspect {

    @Pointcut("execution(* by.tms.lesson48.services.*Service.*(..))")
    public void services() {}

    @Pointcut("execution(* by.tms.lesson48.repositories.*Repository.*(..))")
    public void repositories() {}

    @Around("services() || repositories()")
    public Object aroundServices(ProceedingJoinPoint proceedingJoinPoint) {
        String methodName = proceedingJoinPoint.getSignature().getName();
        String className = proceedingJoinPoint.getSignature().getDeclaringType().getSimpleName();
        log.info("Called method '{}.{}': data {} of starting task", className, methodName, LocalTime.now());
        Object result = null;
        try {
            result = proceedingJoinPoint.proceed();
            log.info("Data {} of '{}.{}' method execution with result: {}", LocalTime.now(), className, methodName, result);
        } catch (Throwable e) {
            log.error("Error {} during service method '{}.{}' call", e.getMessage(), className, methodName);
        }

        return result;
    }

}