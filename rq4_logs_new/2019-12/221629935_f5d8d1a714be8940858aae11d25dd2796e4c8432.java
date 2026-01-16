package net.bluefsd.security;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import feign.RequestInterceptor;
import feign.RequestTemplate;

@Configuration
public class FeignTokenInterceptor implements RequestInterceptor {


    @Override
    public void apply(RequestTemplate template) {

        HttpServletRequest request = getServletRequest();
        if (null == request){
            return;
        }

        template.header("fsdtoken", getToken(request));
    }

    private HttpServletRequest getServletRequest() {
        return ((ServletRequestAttributes)RequestContextHolder.getRequestAttributes()).getRequest();
    }

    private String getToken(HttpServletRequest request){
        return request.getHeader("fsdtoken");
    }


}