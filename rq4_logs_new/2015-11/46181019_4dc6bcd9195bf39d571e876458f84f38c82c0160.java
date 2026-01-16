package com.smapley.powerwork.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import org.xutils.http.app.ResponseParser;
import org.xutils.http.request.UriRequest;

import java.lang.reflect.Type;

/**
 * Created by smapley on 15/11/22.
 */
public class ResultResponseParser implements ResponseParser{
    @Override
    public void checkResponse(UriRequest request) throws Throwable {

    }

    @Override
    public Object parse(Type resultType, Class<?> resultClass, String result) throws Throwable {

        MyResponse response= JSON.parseObject(result,new TypeReference<MyResponse>(){});

        return response;
    }
}