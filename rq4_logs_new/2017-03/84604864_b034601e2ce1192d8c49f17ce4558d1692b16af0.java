package com.alc.alcgit.data;

import com.alc.alcgit.remote.GithubDevSearchAPI;
import com.alc.alcgit.remote.models.Developer;
import com.alc.alcgit.utils.Constants;

import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/**
 * Created by sureife on 10/03/2017.
 */

public  class DevelopersService {

    public interface OnDevelopersLoadedListener{
        void onDevelopersLoaded(List<Developer> developers);

        void onDevelopersLoadFailure(String errorMessage);
    }

    public static void getDevelopers(String searchQualifiers, final OnDevelopersLoadedListener developersLoadedListener){
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(Constants.BASE_URL)
                .addConverterFactory(GsonConverterFactory.create())
                .build();

        GithubDevSearchAPI apiService = retrofit.create(GithubDevSearchAPI.class);

        Call<List<Developer>> call = apiService.getDevelopers(searchQualifiers,Constants.ACCESS_TOKEN);

        call.enqueue(new Callback<List<Developer>>() {
            @Override
            public void onResponse(Call<List<Developer>> call, Response<List<Developer>> response) {
                developersLoadedListener.onDevelopersLoaded(response.body());
            }

            @Override
            public void onFailure(Call<List<Developer>> call, Throwable t) {
                developersLoadedListener.onDevelopersLoadFailure(t.getMessage());
            }
        });




    }
}