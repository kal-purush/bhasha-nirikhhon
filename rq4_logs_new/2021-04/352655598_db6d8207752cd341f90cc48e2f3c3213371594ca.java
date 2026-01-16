package com.kodilla.ecommerce.controller.request;

import com.kodilla.ecommerce.domain.enums.StatusOrder;
import com.kodilla.ecommerce.dto.CartDto;
import com.kodilla.ecommerce.dto.OrderItemDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class CartToOrderDto {

    private Long id;
    private String number;
    @Builder.Default
    private StatusOrder status = StatusOrder.ACCEPTED;
    private String shippingAddress;
    //private LocalDateTime date;
    private CartDto cartDto;
}