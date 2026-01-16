package com.hyep.movietracker.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hyep.movietracker.models.MovieResponse;

import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface ApiService {
    //https://api.themoviedb.org/3/movie/upcoming?api_key=0b9dffdf2cc5ef177909e12da7782207

    Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();
    ApiService apiService = new Retrofit.Builder()
            .baseUrl("https://api.themoviedb.org/3/")
            .addConverterFactory(GsonConverterFactory.create(gson))
            .build()
            .create(ApiService.class);

    @GET("movie/upcoming")
    Call<MovieResponse> getUpcomingMovies (@Query("api_key") String api_key);

}