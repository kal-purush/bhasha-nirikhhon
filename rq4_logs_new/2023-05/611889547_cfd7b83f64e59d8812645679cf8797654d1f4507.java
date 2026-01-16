package com.luximed.gateway.client;

import com.luximed.gateway.model.CustomUserDetails;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@RequiredArgsConstructor
@Service
public class DBClient {
    private static final String DATABASE_URI = "http://localhost:8083/api/database";
    private final RestTemplate template;

    public void savePatient(String pesel){
        template.postForEntity(DATABASE_URI + "/patient/add", pesel, String.class);
    }

    public void savePersonalData(CustomUserDetails customUserDetails){
        template.postForEntity(DATABASE_URI + "/personalData/add", customUserDetails, CustomUserDetails.class);
    }
}