package com.kodilla.good.patterns.challenges.POS.repository;

import com.kodilla.good.patterns.challenges.POS.product.Product;
import com.kodilla.good.patterns.challenges.POS.repository.OrderRepository;
import com.kodilla.good.patterns.challenges.POS.user.User;

import java.time.LocalDateTime;

public class AirplaneOrderRepository implements OrderRepository {

    @Override
    public boolean createOrder(User user, Product product, LocalDateTime dayAndTimeOfAcquisition) {
        return true;
    }
}