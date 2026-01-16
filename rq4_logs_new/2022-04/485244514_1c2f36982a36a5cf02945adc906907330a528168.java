package com.example.blogit.lib.unsplash;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class Unsplash {
    @Value("${unsplash-access-key}")
    private String accessKey;
    private final String BASE_URL = "https://api.unsplash.com/";

    public String getPhoto(String query) {
        final String uri = BASE_URL + "/search/photos?query="+query+"&page=1&per_page=1&client_id=" + accessKey;

        RestTemplate restTemplate = new RestTemplate();
        UnsplashApiResponse result = restTemplate.getForObject(uri, UnsplashApiResponse.class);

        if(result == null) return "";
        return result.getResults().get(0).getUrls().getRegular();
    }

    public String getRandomPhoto() {
        final String uri = BASE_URL + "/photos/random?orientation=landscape&client_id=" + accessKey;

        RestTemplate restTemplate = new RestTemplate();
        UnsplashResults result = restTemplate.getForObject(uri, UnsplashResults.class);

        if(result == null) return "";
        return result.getUrls().getRegular();
    }
}