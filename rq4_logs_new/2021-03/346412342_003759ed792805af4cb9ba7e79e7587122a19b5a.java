package com.hackner.musicompass.discogsapi.service;

import com.hackner.musicompass.secret.DiscogsSecret;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;

@Configuration
public class DiscogsApiEntityService {

    private final DiscogsSecret discogsSecret;

    @Autowired
    public DiscogsApiEntityService(DiscogsSecret discogsSecret) {
        this.discogsSecret = discogsSecret;
    }

    @Bean
    public HttpEntity<Void> createEntity(){
        HttpHeaders headers = new HttpHeaders();
        headers.add("User-Agent", "MusiCompass/0.1");
        headers.add("Authorization", "Discogs token=" + discogsSecret.getDiscogsToken());
        HttpEntity<Void> entity = new HttpEntity<>(headers);
        return entity;
    }

}
