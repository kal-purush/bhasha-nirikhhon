package com.arbaini.getih.utils;

import android.content.Context;
import android.content.SharedPreferences;

public class PrefManager {

    SharedPreferences pref;
    SharedPreferences.Editor editor;
    Context _context;

    // shared pref mode
    int PRIVATE_MODE = 0;

    // Shared preferences file name
    private static final String PREF_NAME = "detailiduser";
    private static final String ID_TOKEN = "token";
    private static final String STATUS_LOGIN = "statuslogin";
    private static final String BT_NAME = "btname";
    private static final String BT_ADDRESS = "btaddress";
    private static final String UPDATE_INTERVAL = "updateinterval";

    public PrefManager(Context context) {
        this._context = context;
        pref = _context.getSharedPreferences(PREF_NAME, PRIVATE_MODE);
        editor = pref.edit();
    }



    public void setIdToken(String idToken) {
        editor.putString(ID_TOKEN, idToken);
        editor.commit();
    }

    public String getIdToken() {
        return pref.getString(ID_TOKEN, "Null");
    }

    public void setBtName(String btName) {
        editor.putString(BT_NAME, btName);
        editor.commit();
    }

    public String getBtName() {
        return pref.getString(BT_NAME, "Null");
    }


    public void setBtAddress(String btAddress) {
        editor.putString(BT_ADDRESS, btAddress);
        editor.commit();
    }

    public String getBtAddress() {
        return pref.getString(BT_ADDRESS, "Null");
    }

    public boolean getStatusLogin() {
        return pref.getBoolean(STATUS_LOGIN,false);
    }

    public void setStatusLogin(boolean statusLogin){
        editor.putBoolean(STATUS_LOGIN,statusLogin);
        editor.commit();
    }


    public int getUpdateInterval(){return pref.getInt(UPDATE_INTERVAL, 0);}

    public void setUpdateInterval(int interval){
        editor.putInt(STATUS_LOGIN,interval);
        editor.commit();
    }
}