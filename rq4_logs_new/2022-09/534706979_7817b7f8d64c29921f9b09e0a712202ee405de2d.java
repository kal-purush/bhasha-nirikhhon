package com.pos.order.controller;

import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.pos.order.entity.Order;
import com.pos.order.service.OrderService;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;


@RunWith(MockitoJUnitRunner.class)
public class OrderControllerTest {
	
	@InjectMocks
    OrderController controller;
	
	@Mock
	OrderService orderService;
	
	@Test
	public void test_fetchOrderlist()
	{	List<Order> orders= new ArrayList<>();
		Order order= new Order();
		order.setOrderId(101L);
		order.setSubscriptionId("SUP01");
		order.setTotalAmount(100.00);
		orders.add(order);
		Mockito.when(orderService.fetchOrderList()).thenReturn(orders);
		assertEquals(1, controller.fetchOrderList().size());
	}
	
	@Test
	public void test_saveOrder()
	{	ResponseEntity<String> re= new ResponseEntity<String>(HttpStatus.CREATED);
		Order order= new Order();
		order.setOrderId(101L);
		order.setSubscriptionId("SUP02");
		order.setTotalAmount(200.00);	
		assertEquals(re,controller.saveOrder(order));
	}
	
	@Test
	public void test_updateOrder()
	{	ResponseEntity<String> re= new ResponseEntity<String>(HttpStatus.ACCEPTED);
		Order order= new Order();
		order.setOrderId(101L);
		order.setSubscriptionId("SUP03");
		order.setTotalAmount(300.00);	
		assertEquals(re,controller.updateOrder(order));
	}
}