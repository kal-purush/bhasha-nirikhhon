package com.hackner.musicompass.discogsapi.service;

import com.hackner.musicompass.secret.DiscogsSecret;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Service
public class DiscogsApiEntityService {

    private DiscogsSecret discogsSecret;

    public HttpEntity getEntity(){
        HttpHeaders headers = new HttpHeaders();
        headers.add("User-Agent","MusiCompass/0.1");
        headers.add("Authorization","Discogs token="+ discogsSecret.getDiscogsToken());
        return new HttpEntity<>(headers);
    }
}