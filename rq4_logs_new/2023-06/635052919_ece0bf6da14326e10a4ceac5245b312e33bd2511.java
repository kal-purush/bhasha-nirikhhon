package com.example.ecommerce.services;

import com.example.ecommerce.model.OrderItem;
import com.example.ecommerce.repos.OrderItemsRepository;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Transactional
public class OrderItemsService {

    @Autowired
    private OrderItemsRepository orderItemsRepository;

    private void addOrderedProducts(OrderItem orderItem) {
        orderItemsRepository.save(orderItem);
    }
}