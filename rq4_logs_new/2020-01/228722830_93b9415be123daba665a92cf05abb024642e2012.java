package com.kodilla.ecommercee.repository;

import com.kodilla.ecommercee.domain.Order;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class OrderRepositoryTestSuite {
    @Autowired
    private OrderRepository orderRepository;

    @Test
    public void testOrderSave() {
        //Given
        Order order = new Order();

        //When
        orderRepository.save(order);
        Long orderId = order.getId();

        //Then
        Long id = order.getId();
        Optional<Order> readOrder = orderRepository.findById(id);
        Assert.assertTrue(readOrder.isPresent());

        //CleanUp
        try {
            orderRepository.deleteById(id);
        } catch (Exception e) {
        }

    }

    @Test
    public void testOrderFindById() {
        //Given
        Order order1 = new Order();
        Order order2 = new Order();
        Order order3 = new Order();
        Order order4 = new Order();

        //When
        orderRepository.save(order1);
        orderRepository.save(order2);
        orderRepository.save(order3);
        orderRepository.save(order4);

        //Then
        Long id = order2.getId();
        Optional<Order> readOrder = orderRepository.findById(id);
        Assert.assertTrue(readOrder.isPresent());

        //CleanUp
        try {
            orderRepository.deleteAll();
        } catch (Exception e) {
        }
    }

    @Test
    public void testOrderFindAll() {
        //Given
        Order order1 = new Order();
        Order order2 = new Order();
        Order order3 = new Order();
        Order order4 = new Order();

        //When
        orderRepository.save(order1);
        orderRepository.save(order2);
        orderRepository.save(order3);
        orderRepository.save(order4);

        //Then
        List<Order> orders = orderRepository.findAll();
        Assert.assertEquals(4, orders.size());
        Assert.assertNotEquals(2, orders.size());

        //CleanUp
        try {
            orderRepository.deleteAll();
        } catch (Exception e) {
        }
    }
}