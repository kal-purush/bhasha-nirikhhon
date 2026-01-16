package com.jinius.ecommerce.order.api.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.jinius.ecommerce.order.application.dto.OrderFacadeResponse;
import com.jinius.ecommerce.order.application.dto.OrderItemFacadeResponse;
import com.jinius.ecommerce.order.domain.model.Order;
import lombok.*;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class OrderResponse {
    private Long orderId;                           //주문 ID
    private String status;                          //주문 상태 [PENDING, PAID, COMPLETED, CANCELED, PARTIAL_REFUND]
    private BigInteger totalPrice;                  //총 주문 가격
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime orderedAt;                //주문 일시
    private List<OrderItemResponse> orderItems;     //주문 상품 리스트

    public static OrderResponse from(OrderFacadeResponse response) {
        return OrderResponse.builder()
                .orderId(response.getOrderId())
                .status(response.getStatus())
                .totalPrice(response.getTotalPrice())
                .orderedAt(response.getOrderedAt())
                .orderItems(response.getOrderItems().stream()
                        .map(OrderItemResponse::from)
                        .collect(Collectors.toList()))
                .build();
    }
}