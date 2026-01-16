package com.xml.feignClients;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;

import java.util.List;

@FeignClient(name = "rent-request-service")
public interface RentRequestFeignClient {
    @GetMapping(value = "/api/rent-request/{id}", headers = {"Authorities=[TEST], Authorization={token}"})
    List<Long> getPeople(@PathVariable("id") Long id, @RequestHeader("token") String token);
}