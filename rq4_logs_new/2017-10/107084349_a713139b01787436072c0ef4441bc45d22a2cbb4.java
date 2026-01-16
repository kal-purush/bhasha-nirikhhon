package com.example.bat_man.utils;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;

import com.example.bat_man.utils.bean.NewsBean;
import com.example.bat_man.utils.net.IonNet;
import com.example.bat_man.utils.net.OkHttpUtilsForPost;
import com.example.bat_man.utils.utils.Api;

import java.util.HashMap;
import java.util.Map;

public class MainActivity extends AppCompatActivity implements IonNet<NewsBean> {
    private Map<String, String> params;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        initView();
        addMap();
        OkHttpUtilsForPost.getInstance().doPost(Api.URLAPI.BASE_URL, params, NewsBean.class);
//        OkHttpUtilsForPost.getInstance().doGet(Api.URLAPI.BASE_URL,params,NewsBean.class);
        OkHttpUtilsForPost.getInstance().onCallBack(this);


    }

    private void addMap() {
        params = new HashMap<>();
        params.put(Api.URLAPI.PARAM_METHOD, Api.URLAPI.METHOD_GET_MUSIC_LIST);
        params.put(Api.URLAPI.PARAM_TYPE, "1");
        params.put(Api.URLAPI.PARAM_SIZE, "10");
        params.put(Api.URLAPI.PARAM_OFFSET, "0");
    }
//

    @Override
    public void onSuccess(NewsBean newsBean) {

        Log.e("aaa", newsBean.getSong_list().get(0).getAlbum_title() + "");
    }


    private void initView() {


    }
}