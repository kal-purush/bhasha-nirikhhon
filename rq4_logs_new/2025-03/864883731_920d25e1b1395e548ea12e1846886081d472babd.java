package com.ecommerce.orderservice.controllers;

import com.ecommerce.orderservice.dtos.AuthorizedUser;
import com.ecommerce.orderservice.dtos.CreateOrderDto;
import com.ecommerce.orderservice.dtos.OrderDto;
import com.ecommerce.orderservice.dtos.OrderItemDto;
import com.ecommerce.orderservice.models.Order;
import com.ecommerce.orderservice.models.OrderItem;
import com.ecommerce.orderservice.services.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.ArrayList;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private OrderService orderService;
    private OrderDto toDto(Order order){
        OrderDto orderDto = new OrderDto();
        orderDto.setId(order.getId());
        orderDto.setCreatedAt(order.getCreatedAt());
        orderDto.setUpdatedAt(order.getUpdatedAt());
        orderDto.setBillingAddress(order.getBillingAddress());
        orderDto.setShippingAddress(order.getShippingAddress());
        orderDto.setPaymentId(order.getPaymentId());
        orderDto.setItems(new ArrayList<>());
        orderDto.setStatus(order.getStatus());
        orderDto.setTotalAmount(order.getTotalAmount());
        orderDto.setUserId(order.getUserId());
        order.getItems().forEach(item -> {
            OrderItemDto orderItemDto = new OrderItemDto();
            orderItemDto.setProductId(item.getProductId());
            orderItemDto.setQuantity(item.getQuantity());
            orderItemDto.setPrice(item.getPrice());
            orderItemDto.setTotalAmount(item.getTotalAmount());
            orderItemDto.setProductDescription(item.getProductDescription());
            orderItemDto.setProductImage(item.getProductImage());
            orderItemDto.setProductTitle(item.getProductTitle());
            orderDto.getItems().add(orderItemDto);
        });
        return orderDto;
    }

    @PostMapping
    public OrderDto createOrder(Authentication authentication, @RequestBody CreateOrderDto createOrderDto) {
        AuthorizedUser authUser = (AuthorizedUser) authentication.getPrincipal();
        Order order = new Order();
        var orderItems = createOrderDto.getItems().stream().map(item -> {
            OrderItem orderItem = new OrderItem();
            orderItem.setProductId(item.getProductId());
            orderItem.setQuantity(item.getQuantity());
            orderItem.setPrice(item.getPrice());
            return orderItem;
        }).collect(Collectors.toList());
        order.setItems(orderItems);
        order = orderService.createOrder(
                authUser,order,
                createOrderDto.getBillingAddressId(),
                createOrderDto.getShippingAddressId(),
                createOrderDto.getPaymentMethod()
        );
        return toDto(order);
    }
    @GetMapping("/{id}")
    public OrderDto getOrder(Authentication authentication, @PathVariable Long id) {
        AuthorizedUser authUser = (AuthorizedUser) authentication.getPrincipal();
        Order order = orderService.getOrder(authUser,id);
        return toDto(order);
    }
}