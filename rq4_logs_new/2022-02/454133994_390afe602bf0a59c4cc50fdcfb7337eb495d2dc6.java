package com.codewithArdents.dysgraphia;

import static android.content.Context.MODE_PRIVATE;

import android.app.Activity;
import android.content.SharedPreferences;

public class Utility {

    public static void setName(Activity activity,String name){
        SharedPreferences sharedPreferences = activity.getSharedPreferences("user", MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putString("name", name).apply();
    }

    public static String getName(Activity activity) {
        SharedPreferences sharedPreferences = activity.getSharedPreferences("user", MODE_PRIVATE);
        return sharedPreferences.getString("name", "");
    }

    public static void setGender(Activity activity,String name){
        SharedPreferences sharedPreferences = activity.getSharedPreferences("user", MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putString("gender", name).apply();
    }

    public static String getGender(Activity activity) {
        SharedPreferences sharedPreferences = activity.getSharedPreferences("user", MODE_PRIVATE);
        return sharedPreferences.getString("gender", "");
    }

    public static void setAge(Activity activity,Integer name){
        SharedPreferences sharedPreferences = activity.getSharedPreferences("user", MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putInt("age", name).apply();
    }

    public static Integer getAge(Activity activity) {
        SharedPreferences sharedPreferences = activity.getSharedPreferences("user", MODE_PRIVATE);
        return sharedPreferences.getInt("name", 1);
    }

    public static void setPhoto(Activity activity,String name){
        SharedPreferences sharedPreferences = activity.getSharedPreferences("user", MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putString("photo", name).apply();
    }

    public static Integer getPhoto(Activity activity) {
        SharedPreferences sharedPreferences = activity.getSharedPreferences("user", MODE_PRIVATE);
        return sharedPreferences.getInt("name", 1);
    }
}