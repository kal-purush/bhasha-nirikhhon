package com.andela.javadevelopers.api;

import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/**
 * Created by andeladeveloper on 29/06/2018.
 */

public class GithubApi {
    private static final String BASE_URL = "https://api.github.com/";
    private static Retrofit retrofit = null;

    public GithubService getClient() {
        if (retrofit == null) {
            retrofit = new Retrofit.Builder()
                    .baseUrl(BASE_URL)
                    .addConverterFactory(GsonConverterFactory.create())
                    .build();
        }
        return retrofit.create(GithubService.class);
    }
}