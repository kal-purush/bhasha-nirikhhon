package com.example.EnjoyTripBackend.controller;

import com.example.EnjoyTripBackend.service.PaymentService;
import com.example.EnjoyTripBackend.util.SessionUser;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @GetMapping("/toss/success")
    public ResponseEntity<String> successPay(@RequestParam("orderId") String orderId, @RequestParam("paymentKey") String paymentKey,
                                                       @RequestParam("amount") long amount , @SessionUser String userEmail) {

        return ResponseEntity.ok().body(paymentService.payment(orderId, paymentKey, amount, userEmail));
    }
}