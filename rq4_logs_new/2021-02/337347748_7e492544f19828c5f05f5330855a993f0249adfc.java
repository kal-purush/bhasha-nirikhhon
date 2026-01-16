package com.tofba.interceptor;

import java.io.IOException;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

/**
 * 
 * <重试拦截器.>
 * 
 * @author Henry(fba02)
 * @version [版本号, 2021-2-9]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class RetryIntercepter implements Interceptor {
    public int maxRetry;// 最大重试次数    
    private int retryNum = 0;// 假如设置为3次重试的话，则最大可能请求4次（默认1次+3次重试）
    
    public RetryIntercepter(int maxRetry) {
        this.maxRetry = maxRetry;
    }
    
    @Override
    public Response intercept(Chain chain)
        throws IOException {
        Request request = chain.request();
        System.out.println("retryNum=" + retryNum);
        Response response = chain.proceed(request);
        if (null != response && response.isSuccessful()) {
            return response;
        }
        while (!response.isSuccessful() && retryNum < maxRetry) {
            retryNum++;
            System.out.println("retryNum=" + retryNum);
            response = chain.proceed(request);
        }
        return response;
    }    
}