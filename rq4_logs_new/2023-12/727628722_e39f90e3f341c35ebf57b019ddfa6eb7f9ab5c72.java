package com.spartanullnull.otil.global.aspect;

import java.util.*;
import lombok.extern.slf4j.*;
import org.aspectj.lang.*;
import org.aspectj.lang.annotation.*;

@Slf4j(topic = "Logging All Of Api Errors")
@Aspect
public class LoggingErrorAspect {

    @Pointcut("execution(* com.spartanullnull.otil.domain.comment.controller.*(..)) || "
        + "execution(* com.spartanullnull.otil.domain.user.controller.*(..)) || "
        + "execution(* com.spartanullnull.otil.domain.category.controller.*(..)) || "
        + "execution(* com.spartanullnull.otil.domain.post.controller.*(..)) || "
        + "execution(* com.spartanullnull.otil.domain.like.controller.*(..)) || "
        + "execution(* com.spartanullnull.otil.domain.recommend.controller.*(..))")
    private void logControllerError() {
    }

    /**
     * 각 API 호출 수행시간 측정
     *
     * @param joinPoint 타겟 API
     * @return 측정시간
     * @throws Throwable 예외발생
     */
    @Around("logControllerError()")
    public Optional<?> logAllOfControllerApiErrors(ProceedingJoinPoint joinPoint)
        throws Throwable {
        Optional<Object> apiRequestResult = Optional.empty();
        try {
            apiRequestResult = Optional.of(joinPoint.proceed());
        } catch (Throwable throwable) {
            log.error("Message of Thrown : " + throwable.getMessage());
            log.error("Cause of Thrown : " + throwable.getCause().getClass());
        }
        return apiRequestResult;
    }
}