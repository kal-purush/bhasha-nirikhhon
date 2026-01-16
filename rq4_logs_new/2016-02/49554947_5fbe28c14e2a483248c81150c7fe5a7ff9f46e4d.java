package com.dremer.framework.mylibrary.okhttp;

import android.os.Handler;
import android.os.Looper;

import com.google.gson.Gson;
import com.google.gson.internal.$Gson$Types;
import com.squareup.okhttp.Call;
import com.squareup.okhttp.Callback;
import com.squareup.okhttp.FormEncodingBuilder;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.util.Map;
import java.util.Set;

/**
 * Created by zx on 2016/2/2.
 */
public class OkHttpClientManager {

    private static OkHttpClientManager mInstance;

    private OkHttpClient mOkHttpClient;
    private Gson mGson;
    private Handler mHandler;

//****************************************私有方法***************************************************
    private OkHttpClientManager() {
        mOkHttpClient = new OkHttpClient();
        mOkHttpClient.setCookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ORIGINAL_SERVER));
        mGson = new Gson();
        mHandler = new Handler(Looper.getMainLooper());
    }

    /**
     * 同步的GET请求。
     *
     * @param url
     * @return
     * @throws IOException
     */
    private Response getSync(String url) throws IOException {
        Request request = new Request.Builder().url(url).build();
        Call call = mOkHttpClient.newCall(request);
        Response response = call.execute();
        return response;
    }

    /**
     * 异步的GET请求。
     *
     * @param url
     * @param callback
     */
    private void getAsync(String url, ResultCallback callback) {
        Request request = new Request.Builder().url(url).build();
        deliveryResult(request, callback);
    }

    private Request buildPostRequest(String url, Param[] params) {
        FormEncodingBuilder builder = new FormEncodingBuilder();
        params = validateParams(params);
        for (Param param : params) {
            builder.add(param.key, param.value);
        }
        RequestBody requestBody = builder.build();
        return new Request.Builder().url(url).post(requestBody).build();
    }

    /**
     * 同步的POST请求。
     *
     * @param url
     * @param params
     * @return
     * @throws IOException
     */
    private Response postSync(String url, Param[] params) throws IOException {
        Request request = buildPostRequest(url, params);
        return mOkHttpClient.newCall(request).execute();
    }

    /**
     * 同步的POST请求。
     *
     * @param url
     * @param maps
     * @return
     * @throws IOException
     */
    private Response postSync(String url, Map<String, String> maps) throws IOException {
        Param[] params = map2Params(maps);
        Request request = buildPostRequest(url, params);
        return mOkHttpClient.newCall(request).execute();
    }

    /**
     * 异步的POST请求。
     *
     * @param url
     * @param params
     * @param callback
     */
    private void postAsync(String url, Param[] params, ResultCallback callback) {
        Request request = buildPostRequest(url, params);
        deliveryResult(request, callback);
    }

    /**
     * 异步的POST请求。
     *
     * @param url
     * @param maps
     * @param callback
     */
    private void postAsync(String url, Map<String, String> maps, ResultCallback callback) {
        Param[] params = map2Params(maps);
        Request request = buildPostRequest(url, params);
        deliveryResult(request, callback);
    }

    private Param[] map2Params(Map<String, String> maps) {
        if (maps == null) {
            return new Param[0];
        }

        int size = maps.size();
        Param[] pairs = new Param[size];
        Set<Map.Entry<String, String>> sets = maps.entrySet();
        int i = 0;
        for (Map.Entry<String, String> entry : sets) {
            pairs[i++] = new Param(entry.getKey(), entry.getValue());
        }
        i = 0;
        return pairs;
    }


    private Param[] validateParams(Param[] params) {
        if (params == null)
            return new Param[0];
        else
            return params;
    }

    /**
     * 发布消息。
     *
     * @param request
     * @param callback
     */
    private void deliveryResult(final Request request, final ResultCallback<String> callback) {
        mOkHttpClient.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Request request, IOException e) {
                sendFailureResultCallback(request, e, callback);
            }

            @Override
            public void onResponse(Response response) throws IOException {
                try {
                    String jsonString = response.body().string();
                    if (callback.mType == String.class) {
                        sendSuccessResultCallback(jsonString, callback);
                    } else {
                        Object obj = mGson.fromJson(jsonString, callback.mType);
                        sendSuccessResultCallback(obj, callback);
                    }
                } catch (Exception e) {
                    sendFailureResultCallback(request, e, callback);
                }
            }
        });
    }

    /**
     * 通过回调向主线程发布错误信息。
     *
     * @param request  请求
     * @param e        异常
     * @param callback 回调接口
     */
    private void sendFailureResultCallback(final Request request, final Exception e, final ResultCallback callback) {
        mHandler.post(new Runnable() {
            @Override
            public void run() {
                if (callback != null)
                    callback.onError(request, e);
            }
        });
    }

    /**
     * 通过回调向主线程发送成功信息。
     *
     * @param obj
     * @param callback
     */
    private void sendSuccessResultCallback(final Object obj, final ResultCallback callback) {
        mHandler.post(new Runnable() {
            @Override
            public void run() {
                if (callback != null)
                    callback.onResponse(obj);
            }
        });
    }

//*****************************************公开方法**************************************************

    /**
     * 同步GET方法。
     *
     * @param url
     * @return
     * @throws IOException
     */
    public static Response requestGetSync(String url) throws IOException {
        return getInstance().getSync(url);
    }

    /**
     * 异步的GET方法。
     *
     * @param url
     * @param callback
     */
    public static void requestGetAsync(String url, ResultCallback callback) {
        getInstance().getAsync(url, callback);
    }

    /**
     * 同步的POST方法。
     *
     * @param url
     * @param maps
     * @return
     * @throws IOException
     */
    public static Response requestPostSync(String url, Map<String, String> maps) throws IOException {
        return getInstance().postSync(url, maps);
    }

    /**
     * 同步的POST方法。
     *
     * @param url
     * @param params
     * @return
     * @throws IOException
     */
    public static Response requestPostSync(String url, Param[] params) throws IOException {
        return getInstance().postSync(url, params);
    }

    /***
     * 异步的POST方法。
     * @param url
     * @param maps
     * @param callback
     */
    public static void requestPostAsync(String url, Map<String, String> maps, ResultCallback callback) {
        getInstance().postAsync(url, maps, callback);
    }

    /***
     * 异步的POST方法。
     * @param url
     * @param params
     * @param callback
     */
    public static void requestPostAsync(String url, Param[] params, ResultCallback callback) {
        getInstance().postAsync(url, params, callback);
    }

    public static OkHttpClientManager getInstance() {
        if (mInstance == null) {
            synchronized (OkHttpClientManager.class) {
                if (mInstance == null) {
                    mInstance = new OkHttpClientManager();
                }
            }
        }
        return mInstance;
    }

//*******************************************内部类**************************************************
    public static abstract class ResultCallback<T> {
        Type mType;

        public ResultCallback() {
            mType = getSuperclassParameter(getClass());
        }

        Type getSuperclassParameter(Class<?> clz) {
            Type superclass = clz.getGenericSuperclass();
            if (superclass instanceof Class) {
                throw new RuntimeException("Missing type parameter.");
            }
            ParameterizedType type = (ParameterizedType) superclass;
            return $Gson$Types.canonicalize(type.getActualTypeArguments()[0]);
        }

        public abstract void onError(Request request, Exception e);

        public abstract void onResponse(T response);
    }

    public static class Param {
        String key;
        String value;

        public Param() {
        }

        public Param(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}