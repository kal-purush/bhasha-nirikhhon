package com.cibertec.proyecto.integrador.controller;

import com.cibertec.proyecto.integrador.entity.Order;
import com.cibertec.proyecto.integrador.entity.OrderDetailEntity;
import com.cibertec.proyecto.integrador.services.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final OrderService orderService;

    @Autowired
    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<?> createOrderWithDetails(@RequestBody Order order) {
        List<OrderDetailEntity> orderDetails = order.getOrderDetails();

        if (orderDetails == null || orderDetails.isEmpty()) {
            return ResponseEntity.badRequest().body("La orden debe tener al menos un detalle.");
        }

        Order createdOrder = orderService.createOrder(order, orderDetails);

        return ResponseEntity.ok(createdOrder);
    }
}
