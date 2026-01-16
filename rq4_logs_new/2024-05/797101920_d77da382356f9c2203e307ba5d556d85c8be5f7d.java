package com.example.EnjoyTripBackend.dto.payment.toss;

import lombok.*;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MyPaymentHistoryDto {

    private String username;
    private String orderName;
    private int totalAmount;
    private String createdDate;
}