package com.v6.yeogaekgi.service;

import com.v6.yeogaekgi.payTrack.dto.PaymentDTO;
import com.v6.yeogaekgi.payTrack.service.PaymentService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Timestamp;

@SpringBootTest
public class PaymentServiceTests {

    @Autowired
    PaymentService paymentService;

    @Test
    void insertPaymentHistory(){

        PaymentDTO dto = PaymentDTO.builder()
                .payType(1)
                .payPrice(17000)
                .payDate(new Timestamp(System.currentTimeMillis()))
                .payBalanceSnap(48000)
                .transitBalanceSnap(17200)
                .status(1)
                .serviceName("Luge")
                .userCardNo(1L  )
                .memberNo(1L)
                .build();
       paymentService.insertPayment(dto);

    }
}