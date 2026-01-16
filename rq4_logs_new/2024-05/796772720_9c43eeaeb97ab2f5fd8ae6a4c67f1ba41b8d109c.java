package com.example.product.orders.controller;

import com.example.product.orders.model.dto.OrderRequestDto;
import com.example.product.orders.model.dto.OrderResponseDto;
import com.example.product.orders.model.service.OrderService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {
    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping()
    public OrderRequestDto addProduct(@RequestBody OrderRequestDto orderRequestDto) {
        orderService.addOrder(orderRequestDto);
        return orderRequestDto;
    }
}