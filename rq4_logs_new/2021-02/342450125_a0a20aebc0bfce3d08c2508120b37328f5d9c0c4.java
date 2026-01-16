package com.zly.service;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(value = "eurekaprovider")
public interface RpcUserInterface {
    @RequestMapping(value = "login", method = RequestMethod.GET)
    String rpcLogin(@RequestParam("userName") String userName, @RequestParam("password") String password);
}