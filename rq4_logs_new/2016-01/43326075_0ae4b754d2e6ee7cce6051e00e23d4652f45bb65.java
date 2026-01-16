package com.example.susha.sensor_collect.Server;

import android.content.Context;
import android.content.SharedPreferences;

import com.jcraft.jsch.UserInfo;

/**
 * Created by DevilFruit on 1/19/2016.
 * see http://www.jcraft.com/jsch/examples/Sftp.java.html for impl
 */
public class MyUserInfo implements UserInfo {
    SharedPreferences sharedPref;
    SharedPreferences.Editor editor;
    public MyUserInfo(Context context){
        sharedPref = context.getSharedPreferences("ServerConfig", Context.MODE_PRIVATE);
        editor = sharedPref.edit();

    }
    @Override
    public String getPassphrase() {
       return null;
    }

    @Override
    public String getPassword() {

        return sharedPref.getString("Password","LAB166");//TODO remove hard code and prompt user
    }

    @Override
    public boolean promptPassword(String message) {
        //TODO Show dialog for password input
        //TODO editor.putString("Password",<User response>);
        return false;
    }

    @Override
    public boolean promptPassphrase(String message) {

        return false;
    }

    @Override
    public boolean promptYesNo(String message) {
        return false;
    }

    @Override
    public void showMessage(String message) {

    }
}