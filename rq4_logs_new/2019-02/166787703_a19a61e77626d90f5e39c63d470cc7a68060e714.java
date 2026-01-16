package com.example.mobile_client;

import android.content.Context;
import android.content.SharedPreferences;


public class MySharedPreferences {


    //create a SharedPreferences named userInfo
    public static SharedPreferences share(Context context){
        SharedPreferences sharedPreferences = context.getSharedPreferences("userInfo", Context.MODE_PRIVATE);
        return sharedPreferences;
    }


    //username
    public static Object getuserName(Context context){
        return share(context).getString("username",null);
    }

    // save username and commit to save
    public static boolean setuserName(String username, Context context){
        SharedPreferences.Editor e = share(context).edit();
        e.putString("username",username);
        Boolean bool = e.commit();
        return bool;
    }

    //password
    public static String getPswd(Context context){
        return share(context).getString("password",null);
    }
    //save
    public static void setPswd(String pswd, Context context){
        SharedPreferences.Editor e = share(context).edit();
        e.putString("password",pswd);
        e.apply();
    }


}