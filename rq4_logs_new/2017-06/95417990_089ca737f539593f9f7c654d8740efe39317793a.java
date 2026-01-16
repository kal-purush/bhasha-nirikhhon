package com.abhishek.myappclient;

import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

/**
 * Created by jawala on 6/26/2017.
 */
@Service
public class ClientService {

    @Autowired
    private RestTemplate restTemplate;

    // Note: Hystrix command works only with @Component or @Service
    @HystrixCommand(fallbackMethod = "helloFallback")
    public String getHelloServer() {
        String url = "http://MYAPP-SERVER/api/server";
        return restTemplate.getForObject(url, String.class);
    }

    public String helloFallback(Throwable hystrixException) {
        // Throwable obj will have the actual error which caused for fallback
        return "fallback hello";
    }
}