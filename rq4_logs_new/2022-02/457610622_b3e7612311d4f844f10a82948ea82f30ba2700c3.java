package com.example.myapplication.bean;

import android.util.Log;

/**
 * Hook辅助类
 */
public class HookBean {

    public static final String TAG = HookBean.class.toString();

    private HookPriClass hookPriClass;

    private final int id;

    public HookBean() {
        hookPriClass = new HookPriClass();
        id = hookPriClass.hashCode();
    }

    public void show(){
        /**
         * 如果想防止被hook，加判断记录原始的hashcode，每次执行前判断
         */
        if(hookPriClass.hashCode()==id){
            hookPriClass.showName();
        }else {
            Log.d(TAG, "show: the class is changed,waring!");
        }
    }

    public void show2(){
        hookPriClass.showName();
    }

    public static class HookPriClass{
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public HookPriClass() {
            name = "zhangsan";
        }

        public void showName(){
            Log.d(TAG, "showName: the name is " + name);
        }


    }
}