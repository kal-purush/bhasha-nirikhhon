package org.example.config.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.example.common.annotation.Log;
import org.example.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Aspect
@Component
public class LogAspect {

    public static final String LOG_TEMPLATE_WITHOUT_DESC = "Func [{}], params: {}, result: {}, time: {}ms";
    public static final String LOG_TEMPLATE_WITH_DESC = "Func [{}], desc: {}, params: {}, result: {}, time: {}ms";
    public static final String LOG_FAILURE_TEMPLATE = "Func [%s] occur exception, params: %s";

    @Around("@annotation(log)")
    public Object around(ProceedingJoinPoint joinPoint, Log log) throws Throwable {
        MethodSignature signature = ((MethodSignature) joinPoint.getSignature());
        Logger logger = LoggerFactory.getLogger(signature.getDeclaringType());

        String methodName = signature.getName();
        String[] paramNames = signature.getParameterNames();
        Map<String, Object> argMap = dealWithArgs(paramNames, joinPoint.getArgs());
        String params = JsonUtil.toJSONString(argMap);
        Object result;
        try {
            long start = System.currentTimeMillis();
            result = joinPoint.proceed();
            long end = System.currentTimeMillis();
            if (StringUtils.isEmpty(log.desc())) {
                logger.info(LOG_TEMPLATE_WITHOUT_DESC, methodName, params, JsonUtil.toJSONString(result), end - start);
            } else {
                logger.info(LOG_TEMPLATE_WITH_DESC, methodName, log.desc(), params, JsonUtil.toJSONString(result), end - start);
            }
        } catch (Throwable throwable) {
            logger.error(String.format(LOG_FAILURE_TEMPLATE, methodName, params), throwable);
            throw throwable;
        }
        return result;
    }

    private Map<String, Object> dealWithArgs(String[] parameters, Object[] args) {
        if (Objects.isNull(args) || args.length == 0) {
            return null;
        }

        Map<String, Object> valueMap = new HashMap<>(16);
        for (int index = 0, len = args.length; index < len; index++) {
            Object arg = args[index];
            if (arg instanceof MultipartFile) {
                continue;
            }
            if (arg instanceof HttpServletRequest) {
                continue;
            }
            if (arg instanceof HttpServletResponse) {
                continue;
            }

            valueMap.put(parameters[index], arg);
        }
        return valueMap;
    }


}