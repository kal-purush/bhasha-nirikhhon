package com.modsen.discoveryservice.aspect;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.Arrays;
import java.util.Objects;

@Slf4j
@AllArgsConstructor
@Component
@Aspect
public class RequestResponseLogger {

    private final ObjectMapper objectMapper;

    private static final String REQUEST_LOGGING_MESSAGE = "Request: Method: {}, URI: {}, Body: {}";
    private static final String RESPONSE_LOGGING_MESSAGE = "Response: Method: {}, URI: {}, Body: {} Time Taken: {} ms";
    private static final String ERROR_SERIALIZING_JSON_MESSAGE = "Error serializing object to JSON";
    private static final String ERROR_SERIALIZING_REQUEST_BODY_MESSAGE = "Error serializing request body";

    @Around("@within(org.springframework.web.bind.annotation.RestController) && execution(* *(..))")
    public Object logHttpRequestResponse(ProceedingJoinPoint joinPoint) throws Throwable {
        HttpServletRequest request =
                ((ServletRequestAttributes) Objects.requireNonNull(RequestContextHolder.getRequestAttributes())).getRequest();
        String requestBody = getRequestBody(joinPoint);
        log.info(REQUEST_LOGGING_MESSAGE,
                request.getMethod(),
                request.getRequestURI(),
                requestBody
        );
        return getResponse(joinPoint, request);
    }

    @Around("@within(org.springframework.web.bind.annotation.RestControllerAdvice) && execution(* *(..))")
    public Object logHttpRequestResponseIfGlobalExceptionHandler(ProceedingJoinPoint joinPoint) throws Throwable {
        HttpServletRequest request =
                ((ServletRequestAttributes) Objects.requireNonNull(RequestContextHolder.getRequestAttributes())).getRequest();
        return getResponse(joinPoint, request);
    }

    private Object getResponse(ProceedingJoinPoint joinPoint, HttpServletRequest request)
            throws Throwable {
        long startTime = System.currentTimeMillis();
        long endTime;
        Object response;
        response = joinPoint.proceed();
        endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        String responseBody = convertObjectToJson(response);
        log.info(RESPONSE_LOGGING_MESSAGE,
                request.getMethod(),
                request.getRequestURI(),
                responseBody,
                duration
        );
        return response;
    }

    private String getRequestBody(ProceedingJoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        if (args.length > 0) {
            try {
                return Arrays.stream(args)
                        .map(this::convertObjectToJson)
                        .reduce((arg1, arg2) -> arg1 + ", " + arg2)
                        .orElse("");
            } catch (Exception e) {
                log.error(ERROR_SERIALIZING_REQUEST_BODY_MESSAGE, e);
            }
        }
        return "";
    }

    private String convertObjectToJson(Object object) {
        if (object == null)
            return "";
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (Exception e) {
            log.error(ERROR_SERIALIZING_JSON_MESSAGE, e);
            return ERROR_SERIALIZING_JSON_MESSAGE;
        }
    }

}