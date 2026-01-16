package com.zx.gitdroid.utils;

import android.os.AsyncTask;
import android.util.Log;

import com.zx.gitdroid.fragment.ItemFrgment;

import java.util.ArrayList;

/**
 * Created by keepRun on 2016/7/28.
 */
public class ItemFragmentPersenter {
    private ItemFrgment fragment;
    private int count;

    public ItemFragmentPersenter(ItemFrgment fragment) {
        this.fragment =  fragment;
    }

    //刷新数据的业务逻辑方法
    public void onRefrash(){
        new refrashTask().execute();
    }
    //异步线程去拿数据
    class refrashTask extends AsyncTask<Void,Void,Void>{
        @Override
        protected Void doInBackground(Void... voids) {
            try {
                //模拟实现，拿数据耗时操作
                Thread.sleep(1500);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }

        //返回数据
        @Override
        protected void onPostExecute(Void aVoid) {
            super.onPostExecute(aVoid);
            ArrayList<String> datas = new ArrayList<>();
            for(int i =0 ;i<20 ;i++){
                datas.add("测试数据"+(count++));
            }
            fragment.stopRefresh();
            fragment.refreshData(datas);
        }
    }
}