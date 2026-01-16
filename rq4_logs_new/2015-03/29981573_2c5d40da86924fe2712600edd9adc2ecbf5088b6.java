package com.anda.rssreader;

import android.content.Context;
import android.view.View;

import java.util.List;

/**
 * Created by anda on 3/23/2015.
 */
public class MainActivityAdapter {

    private Context context;
    private List<WebSite> webSiteList;


    public MainActivityAdapter(Context context, List<WebSite> websiteList){
        this.context = context;
        this.webSiteList = websiteList;
    }

     public class ViewHolder {

        public ViewHolder(){

      }


    }


}