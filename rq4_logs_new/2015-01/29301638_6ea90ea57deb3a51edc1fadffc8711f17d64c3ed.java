package com.silasapp.activity;

/**
 * Created by Silas on 2015/1/12.
 */
public interface ISilasActivity {
    void init();    //初始化数据

    void initView();    //初始化界面控件

    void refresh(Object ... param);  //刷新内容
}