package com.cakestation.backend.user.service;

import java.io.*;
import java.net.*;
import java.nio.Buffer;

import javax.servlet.http.Cookie;

import com.cakestation.backend.user.dto.response.KakaoUserDto;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class HttpUtil {
    //query params에 데이터값을 넣어줘야 할경우
    //최초 토큰을 받아올때
    //refresh Token을 받아올때 
    public static JsonElement getTokenurl(String targetUrl) throws IOException {

        JsonElement element = null;

        try {
            URL url = new URL(targetUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            //POST 요청을 위해 기본값이 false인 setDoOutput을 true로
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            int responseCode = con.getResponseCode();
            BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));

            //받은 값들을 확인
            String line = "";
            String result = "";
            while ((line = br.readLine()) != null) {
                result += line;
            }
            //JSON데이터를 확인
            JsonParser parser = new JsonParser();
            element = parser.parse(result);

        } catch (IOException e) {
            e.printStackTrace();
        } 
        
        return element;
    }
    
    //Header에 Authorization으로 토큰값을 Bearer정책에 맞춰 전달해야할 경우 
    //로그아웃
    //유저 정보찾기
    //회원탈퇴
    public static JsonElement sendKakao(String targetUrl,String checkValue) throws IOException {

        JsonElement element = null;

        try {
            URL url = new URL(targetUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            //POST 요청을 위해 기본값이 false인 setDoOutput을 true로
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Authorization", "Bearer " + checkValue);

            int responseCode = con.getResponseCode();
            BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));

            //받은 값들을 확인
            String line = "";
            String result = "";
            while ((line = br.readLine()) != null) {
                result += line;
            }
            //JSON데이터를 확인
            JsonParser parser = new JsonParser();
            element = parser.parse(result);

        } catch (IOException e) {
            e.printStackTrace();
        } 
        
        return element;
    }
    
}