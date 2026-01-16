package com.wuqiyan.shuzz.net;

import java.util.Map;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.QueryMap;

/**
 * Created by wuqiyan on 17/7/31.
 */

public interface MobApi {
    @GET("http://apicloud.mob.com/ucache/put")
    Call<ResponseBody> putMobKV(@QueryMap Map<String,String> params);
}