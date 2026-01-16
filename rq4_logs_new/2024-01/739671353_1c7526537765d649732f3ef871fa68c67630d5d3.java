package com.fpr.kakao;

import lombok.Data;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

@Data
public class KakaoApi {
    // class KakaoApi
    @Value("${kakao.api_key}")
    private String kakaoApiKey;

    @Value("${kakao.redirect_uri}")
    private String kakaoRedirectUri;

    public String getKakaoAccessToken(String code) throws ParseException {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("grant_type", "authorization_code");
        params.add("client_id", kakaoApiKey);
        params.add("redirect_uri", kakaoRedirectUri);
        params.add("code", code);

        // http 헤더 고정
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-type", "application/x-www-form-urlencoded;charset=utf-8");

        //http 헤더와 바디 합치기
        HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<>(params, headers);

        //http post 요청 보내기
        RestTemplate rt = new RestTemplate();
        ResponseEntity<String> response = rt.exchange(
                "https://kauth.kakao.com/oauth/token", //{요청할 서버 주소}
                HttpMethod.POST, //{요청할 방식}
                entity, // {요청할 때 보낼 데이터}
                String.class
        );

        String responseBody = response.getBody();
        JSONParser parser = new JSONParser();
        JSONObject object = (JSONObject) parser.parse(responseBody);
        String accessToken = (String) object.get("access_token");
        return accessToken;
    }

    public ResponseEntity<String> getUserInfo(String accessToken) {

        // http 헤더
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-type", "application/x-www-form-urlencoded;charset=utf-8");
        headers.add("Authorization", "Bearer "+accessToken);

        //http 헤더와 바디 합치기
        HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<>(headers);

        //http post 요청 보내기
        RestTemplate rt = new RestTemplate();
        ResponseEntity<String> response = rt.exchange(
                "https://kapi.kakao.com/v2/user/me", //{요청할 서버 주소}
                HttpMethod.POST, //{요청할 방식}
                entity, // {요청할 때 보낼 데이터}
                String.class
        );
        return response;
    }
}