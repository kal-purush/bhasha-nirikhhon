package com.spotifyapi.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import se.michaelthelin.spotify.exceptions.detailed.TooManyRequestsException;

@Aspect
@Component
@Slf4j
public class RetryAfterCatchAspect {

    @Around("@annotation(com.spotifyapi.customAnnotation.RetryAfterRequest)")
    public Object handleTooManyRequests(ProceedingJoinPoint joinPoint) throws Throwable {
        while (true) {
            try {
                return joinPoint.proceed();
            } catch (TooManyRequestsException e) {
                int retryAfter = e.getRetryAfter();
                log.warn("Too many requests. Retrying after {} seconds...", retryAfter);
                Thread.sleep(retryAfter * 1000L);
            }
        }
    }
}