package com.h33kz.WarehouseManagementSystem.controllers;

import java.awt.List;
import java.util.LinkedList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.h33kz.WarehouseManagementSystem.models.Customer;
import com.h33kz.WarehouseManagementSystem.models.Order;
import com.h33kz.WarehouseManagementSystem.models.OrderedItem;
import com.h33kz.WarehouseManagementSystem.models.Product;
import com.h33kz.WarehouseManagementSystem.service.OrderService;

@RestController
@RequestMapping("/order")
public class OrderController {
  
  @Autowired
  private OrderService orderService;
  
  @PostMapping("/insert")
  public Order insertOrder(@RequestBody Order order){
    // Product mockProduct =new Product();
    // Customer mockCustomer = new Customer();
    // OrderedItem mockOrderItem = new OrderedItem();
    // mockProduct.setId(1);
    // mockCustomer.setId(1);
    // mockOrderItem.setQuantity(1);
    // mockOrderItem.setProduct(mockProduct);
    //
    // Order mockOrder = new Order();
    // mockOrder.setCustomer(mockCustomer);
    //
    // LinkedList<OrderedItem> mockOrderedItems = new LinkedList<>();
    // mockOrderedItems.add(mockOrderItem);
    //
    // mockProduct =new Product();
    // mockOrderItem = new OrderedItem();
    // mockProduct.setId(2);
    // mockOrderItem.setQuantity(1);
    // mockOrderItem.setProduct(mockProduct);
    //
    // mockOrderedItems.add(mockOrderItem);
    //
    // mockOrder.setOrderedItems(mockOrderedItems);
    // return orderService.insertOrder(mockOrder);
    return orderService.insertOrder(order);
  }

  @GetMapping("/getWith/{id}")
  public Order getOrderWithId(@PathVariable int id){
    return orderService.getOrderById(id);
  }
  
}