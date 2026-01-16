package com.ecommerce.cartservice.dtos;

import com.ecommerce.cartservice.enums.PaymentMethod;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import java.util.List;

@Getter
@Setter
public class CreateOrderDto {
    @NotNull
    private Long shippingAddressId;
    
    @NotNull
    private Long billingAddressId;
    
    private PaymentMethod paymentMethod;
    
    @NotEmpty(message = "The items must not be empty")
    private List<CreateOrderItemDto> items;
} 