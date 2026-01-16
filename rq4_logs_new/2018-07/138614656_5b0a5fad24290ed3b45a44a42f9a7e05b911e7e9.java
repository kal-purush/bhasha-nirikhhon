package com.andela.javadevelopers.home.model;

import android.support.annotation.NonNull;
import android.support.annotation.Nullable;

import com.andela.javadevelopers.contract.MainContract;
import com.andela.javadevelopers.network.GithubApi;

import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

/**
 * Created by andeladeveloper on 18/07/2018.
 */

public class GetGithubUserIntractor implements MainContract.GetGithubIntractor {
    /**
     * The Github api.
     */
    GithubApi githubApi = new GithubApi();
    /**
     * The List items.
     */
    List<GithubUsers> listItems;

    @Override
    public void getGithubList(final OnFinishedListener onFinishedListener) {
        githubApi
                .getClient()
                .getItems()
                .enqueue(new Callback<GithubUsersResponse>() {

                    @Override
                    public void onResponse(@NonNull Call<GithubUsersResponse> call,
                                           @Nullable Response<GithubUsersResponse> response) {
                        if (response != null) {
                            listItems = response.body().getGithubUsers();
                            onFinishedListener.onFinished(listItems);
                        }
                    }

                    @Override
                    public void onFailure(@NonNull Call<GithubUsersResponse> call,
                                          @NonNull Throwable t) {
                        onFinishedListener.onFailure(t);
                    }
                });


    }
}