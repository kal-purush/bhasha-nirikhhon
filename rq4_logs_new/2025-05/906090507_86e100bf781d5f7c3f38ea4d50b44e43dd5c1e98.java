package org.dmytr.productOrderMvc.controller;

import lombok.AllArgsConstructor;
import org.dmytr.productOrderMvc.model.Order;
import org.dmytr.productOrderMvc.service.OrderService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/orders")
@AllArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping("/add")
    public void addOrder(@RequestBody Order order) {
        orderService.createOrder(order);
    }
    @GetMapping("/getAll")
    public  Map<Integer,Order> getOrders() {
        return orderService.getOrders();
    }

    @GetMapping("/getByIdParam/{id}")
    public  Order getByIdParam(@PathVariable int id) {
        return orderService.getOrderById(id);
    }

    @GetMapping("/getByIdRequest")
    public  Order getByIdRequest(@RequestParam int id) {
        return orderService.getOrderById(id);
    }



}