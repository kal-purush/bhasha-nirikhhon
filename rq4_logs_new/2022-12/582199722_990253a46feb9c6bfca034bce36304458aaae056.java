package com.qiniu.qnnouidemo;

import android.app.Application;
import android.content.Context;
import android.util.Log;

import com.qiniu.qnnouidemo.uitil.AppCache;
import com.qiniu.qnnouidemo.uitil.BZkIToken;
import com.qlive.core.QLiveCallBack;
import com.qlive.core.QLiveConfig;
import com.qlive.core.QTokenGetter;
import com.qlive.qnlivekit.uitil.JsonUtils;
import com.qlive.qnlivekit.uitil.OKHttpManger;
import com.qlive.qnlivekit.uitil.UserManager;
import com.qlive.sdk.QLive;

import okhttp3.Call;
import okhttp3.Request;
import okhttp3.Response;

public class DemoApplication extends Application {
    private static final String TAG = "DemoApplication";
    public static final String demo_url = "https://niucube-api.qiniu.com";

    @Override
    protected void attachBaseContext(Context base) {
        super.attachBaseContext(base);

    }

    @Override
    public void onCreate() {
        super.onCreate();
        AppCache.setContext(this);
        Log.d("登录：","QLive.init");
        QLive.init(this, new QLiveConfig(), new QTokenGetter() {
            @Override
            public void getTokenInfo(QLiveCallBack<String> callback) {
                Log.d("登录：","QLive.init.getTokenInfo");
                Log.d("QTokenGetter", "QTokenGetter3 ");
                //业务方获取token
                Log.d("QTokenGetter", "QTokenGetter "+UserManager.INSTANCE.getUser().data.loginToken);
                getLoginToken(callback);
            }
        });

    }
    //demo获取token
    private void getLoginToken(QLiveCallBack<String> callBack ) {
        new Thread(() -> {
            try {
                Log.d("登录：","getLoginToken");
                Request requestToken = new Request.Builder()
                        .url(DemoApplication.demo_url+"/v1/live/auth_token?userID="+UserManager.INSTANCE.getUser().data.accountId+"&deviceID=adjajdasod")
                        .addHeader("Content-Type", "application/json")
                        .addHeader("Authorization", "Bearer " + UserManager.INSTANCE.getUser().data.loginToken)
                        .get()
                        .build();
                Call callToken = OKHttpManger.INSTANCE.getOkHttp().newCall(requestToken);
                Response repToken = callToken.execute();
                String tkjson = repToken.body().string();
                BZkIToken tkobj = JsonUtils.INSTANCE.parseObject(tkjson, BZkIToken.class);
                Log.d("登录："," callBack.onSuccess");
                callBack.onSuccess(tkobj.data.accessToken);
            } catch (Exception e) {
                e.printStackTrace();
                callBack.onError(-1, "");
            }
        }).start();
    }
}