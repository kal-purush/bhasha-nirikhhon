package com.ssu.commerce.book.controller;

import com.ssu.commerce.book.grpc.GrpcClientService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequiredArgsConstructor
public class GrpcController {

    private final GrpcClientService grpcClientService;

    @GetMapping("/grpc-test")
    public String printMessage() {
        return grpcClientService.sendMessage("test");
    }
}