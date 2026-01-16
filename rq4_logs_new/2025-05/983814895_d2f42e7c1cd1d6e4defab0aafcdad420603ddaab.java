package com.example.orderservice.controller;

import com.example.orderservice.controller.dto.OrderRequest.CreateOrderRequest;
import com.example.orderservice.controller.dto.OrderResponse.CommonOrderResponse;
import com.example.orderservice.controller.dto.OrderResponse.GetOrdersResponse;
import com.example.orderservice.mapper.OrderMapper;
import com.example.orderservice.service.OrderService;
import com.example.orderservice.service.dto.CreateOrderCommand;
import com.example.orderservice.service.dto.Order;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RequestMapping(value = "/order-service")
@RestController
public class OrderController {

    private final OrderService orderService;
    private final OrderMapper orderMapper;
    private final Environment env;

    @GetMapping("/health-check")
    public String status() {
        return String.format("It's Working in Catalog Service on Port %s",
            env.getProperty("local.server.port"));
    }

    @PostMapping("/{userId}/orders")
    public ResponseEntity<CommonOrderResponse> createOrder(@PathVariable String userId,
        @RequestBody CreateOrderRequest request) {
        CreateOrderCommand createOrderCommand = orderMapper.toCreateOrderCommand(request, userId);
        Order order = orderService.createOrder(createOrderCommand);

        CommonOrderResponse createOrderResponse = orderMapper.toCreateOrderResponse(order);
        return ResponseEntity.status(HttpStatus.CREATED)
            .body(createOrderResponse);
    }

    @GetMapping("/{userId}/orders")
    public ResponseEntity<GetOrdersResponse> getOrdersByUserId(@PathVariable String userId) {
        List<Order> orders = orderService.getOrdersByUserId(userId);
        GetOrdersResponse getOrdersResponse = orderMapper.toGetOrdersResponse(orders);

        return ResponseEntity.status(HttpStatus.OK).body(getOrdersResponse);
    }
}