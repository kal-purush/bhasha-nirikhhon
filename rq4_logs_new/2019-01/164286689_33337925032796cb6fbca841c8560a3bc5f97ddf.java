package com.andlausy.githubrepos.repo_details.data.cloud;

import com.andlausy.githubrepos.repo_details.data.models.Subscriber;

import java.util.List;

import io.reactivex.Observable;
import retrofit2.http.GET;
import retrofit2.http.Url;

public interface RepositoryDetailService {

    @GET
    Observable<List<Subscriber>> getRepoSubscribers(@Url String url);
}