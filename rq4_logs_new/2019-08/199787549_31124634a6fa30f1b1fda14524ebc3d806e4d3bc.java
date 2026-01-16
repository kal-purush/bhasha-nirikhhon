package com.example.myapplication.model;

import com.example.myapplication.model.parsingJson.ApiUser;
import com.example.myapplication.model.parsingJson.AuthUser;
import com.example.myapplication.model.parsingJson.RegTel;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Query;

public interface ApiRequests {
    @GET("sms/request")
    Call<RegTel> regTel(@Query("phone") long telNumber);

    @POST("clients/register/?")
    Call<AuthUser> authUser(@Query("tel_num") int telNumber, @Query("sms") int sms);

    @GET("clients/1/")
    Call<ApiUser> getClient();
}