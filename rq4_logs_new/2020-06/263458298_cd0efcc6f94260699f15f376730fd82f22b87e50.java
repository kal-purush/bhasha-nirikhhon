package com.xml.feignClients;

import com.xml.dto.UserDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;

@FeignClient(name = "authentication-service")
public interface UserFeignClient {

    @PostMapping(value = "/api/physical", headers = {"Authorization={token}"})
    Long createPhysicalUser(@RequestBody UserDto userDto, @PathVariable("token") String token);
}