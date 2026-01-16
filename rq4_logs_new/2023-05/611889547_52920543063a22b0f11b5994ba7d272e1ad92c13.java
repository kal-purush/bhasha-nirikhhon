package com.luximed.gateway.client;

import com.luximed.gateway.config.CustomUserDetails;
import com.luximed.gateway.model.UserCredential;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
public class DatabaseClient {

    private static final String DATABASE_URL = "http://localhost:8083/api/database";

    private final WebClient webClient;

    public DatabaseClient(WebClient.Builder builder) {
        webClient = builder.baseUrl(DATABASE_URL).build();
    }

//    @GetMapping(value = "api/database/personalData/{pesel}")
//    UserCredential getUserCredentialsByPesel(@PathVariable String pesel);
//
//    @PostMapping(value = "api/database/personalData/add")
//    void addPersonalData(@RequestParam String pesel,
//                         @RequestParam Gender gender,
//                         @RequestParam String mail,
//                         @RequestParam String name,
//                         @RequestParam String surname,
//                         @RequestParam String phone,
//                         @RequestParam String password);
//
//    @PostMapping(value = "api/database/patient/add")
//    void addPatient(@RequestParam String pesel);
}