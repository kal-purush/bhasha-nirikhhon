package com.app.laptrinhdidong.API;

import com.app.laptrinhdidong.model.DanhMuc;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


import java.util.ArrayList;

import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.http.GET;

public interface ApiService {
    Gson GSON = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    ApiService apiService= new Retrofit.Builder()
            .baseUrl("https://androidgreenmarketphuc.herokuapp.com/")
            .addConverterFactory(GsonConverterFactory.create(GSON))
            .build()
            .create(ApiService.class);


    @GET("api/danh-mucs")
    Call<ArrayList<DanhMuc>> convertDanhMuc();

}