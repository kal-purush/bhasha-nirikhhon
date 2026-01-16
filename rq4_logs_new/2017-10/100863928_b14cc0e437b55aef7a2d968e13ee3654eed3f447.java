package com.foodworld.restclient;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class RestClient {

    public String getService(String uri) {
        RestTemplate restTemplate = new RestTemplate();
        String result = restTemplate.getForObject(uri, String.class);
        // System.out.println(result);
        return result;
    }
    
    /*public static void main(String[] args) {
        String s= "https://www.smsstriker.com/API/sms.php?from=MEONWH&username=Raja Maragani&password=cDaX7aABb&to=9502449596&msg=Test Otp : 1415&type=1";
        getService(s);
    }*/
}