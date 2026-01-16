package com.shaq.zmd_test.api;

import com.shaq.zmd_test.model.CatsPojo;
import com.shaq.zmd_test.model.CatsRealm;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.schedulers.Schedulers;

public class ServerCommunicator {

    private static final int DEFAULT_TIMEOUT = 10; // seconds
    private static final long DEFAULT_RETRY_ATTEMPTS = 4;
    private static final String TAG = ServerCommunicator.class.getSimpleName();
    private ApiService service;

    public ServerCommunicator(ApiService service) {
        this.service = service;
    }

    public Single<List<CatsRealm>> getCats() {
        return service.getCats()
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .timeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS)
                .retry(DEFAULT_RETRY_ATTEMPTS);
    }
}