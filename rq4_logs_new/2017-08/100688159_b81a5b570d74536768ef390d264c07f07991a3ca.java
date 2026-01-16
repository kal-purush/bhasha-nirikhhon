package com.github.mrmitew.skeleton.data.http;

import android.content.Context;

import java.io.File;

import javax.inject.Inject;
import javax.inject.Singleton;

import okhttp3.Cache;

@Singleton
public class OkHttpCache {
    private Context mContext;
    private Cache mCache;

    @Inject
    public OkHttpCache(Context context) {
        mContext = context;
    }

    public Cache create() {
        mCache = null;
        try {
            mCache = new Cache(new File(mContext.getCacheDir(), "http-mCache"),
                    10 * 1024 * 1024); // 10 MB
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mCache;
    }

    public Cache getCache() {
        return mCache;
    }
}