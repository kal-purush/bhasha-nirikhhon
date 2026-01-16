package com.app.bookstore.backend.dto;

import lombok.*;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class OrderDTO
{
    private int quantity;
    private double price;
    private Long addressId;
}