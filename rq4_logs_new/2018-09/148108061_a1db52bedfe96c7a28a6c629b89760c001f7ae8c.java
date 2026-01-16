package com.selenium;


import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        HttpURLConnection connection = null;
        try {
            URL u = new URL("http://www.google.com/");
            connection = (HttpURLConnection) u.openConnection();
            connection.setRequestMethod("HEAD");
            int code = connection.getResponseCode();
            System.out.println("Please wait chekcing your URL..... ");
            System.out.println("" + code);
            if (code == 200){
                System.out.println("UP");

            }else{
                System.out.println("Down");
            }

            // You can determine on HTTP return code received. 200 is success.
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block

            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.disconnect();

            }
        }

    }
}