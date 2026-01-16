package com.pos.order.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.pos.order.entity.Order;
import com.pos.order.service.OrderService;


@RestController
public class OrderController {

	@Autowired
	private OrderService orderService;
	
	@PostMapping(value = "/order", consumes = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> saveOrder(@RequestBody Order order){
		System.out.println("---------"+order);
		orderService.saveOrder(order);
		return new ResponseEntity<String>(HttpStatus.CREATED);
	}
	
	@PutMapping(value = "/order", consumes = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> updateOrder(@RequestBody Order order){
		System.out.println("---------"+order);
		orderService.updateOrder(order);
		return new ResponseEntity<String>(HttpStatus.ACCEPTED);
	}
	
	@GetMapping(value = "/orderList", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<Order> fetchOrderList(){
		return orderService.fetchOrderList();
	}
	
}