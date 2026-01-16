package com.github.hcsp.starter;


import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestResponseBodyMethodProcessor;


/**
 * @author gxz gongxuanzhang@foxmail.com
 **/

public class MyResponseAnalysis implements HandlerMethodReturnValueHandler, ApplicationContextAware {


    private RequestResponseBodyMethodProcessor delegate;
    private ApplicationContext applicationContext;

    @Override
    public boolean supportsReturnType(MethodParameter returnType) {
        return AnnotatedElementUtils.hasAnnotation(returnType.getContainingClass(), MyResponseBody.class)
                || returnType.hasMethodAnnotation(MyResponseBody.class);
    }

    @Override
    public void handleReturnValue(Object returnValue, MethodParameter returnType, ModelAndViewContainer mavContainer,
                                  NativeWebRequest webRequest) throws Exception {
        if (delegate == null) {
            delegate =
                    (RequestResponseBodyMethodProcessor) applicationContext.getBean(RequestMappingHandlerAdapter.class).getReturnValueHandlers()
                            .stream()
                            .filter(p -> p instanceof RequestResponseBodyMethodProcessor)
                            .findFirst().get();
        }
        delegate.handleReturnValue(new MyResponseResult("ok", returnValue), returnType,
                mavContainer, webRequest);
    }


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}