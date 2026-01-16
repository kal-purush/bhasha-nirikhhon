package com.wcalendar.klp.wcalendar;

import java.net.HttpURLConnection;
import java.net.URL;

public class Internet_state_check extends Thread {
    private boolean success;
    private String host;

    public Internet_state_check(String host){
        this.host = host;
    }

    @Override
    public void run() {
        HttpURLConnection conn = null;
        try{
            conn = (HttpURLConnection) new URL(host).openConnection();
            conn.setRequestProperty("User-Agent","Android");
            conn.setConnectTimeout(1000);
            conn.connect();
            int responseCode = conn.getResponseCode();
            if(responseCode == 204){
                success = true;
            }else{
                success = false;
            }
        }catch(Exception e){
            success = false;
            e.printStackTrace();
        }
        if(conn != null){
            conn.disconnect();
        }
        super.run();
    }

    public boolean isSuccess() {
        return success;
    }
}