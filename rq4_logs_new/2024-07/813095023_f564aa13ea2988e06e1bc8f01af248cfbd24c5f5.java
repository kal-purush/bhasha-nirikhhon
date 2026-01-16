package com.spring.mood.projectmvc.service;

import com.spring.mood.projectmvc.mapper.OrderDetailsMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderDetailsService {

    private final OrderDetailsMapper orderDetailsMapper;

    public void saveOrderDetails(Long orderId, List<Long> cartArchiveIds) {
        log.info("Saving order details with Order ID: {}", orderId);  // 로그를 통해 orderId 확인
        orderDetailsMapper.saveOrderDetails(orderId, cartArchiveIds);
    }

    public List<Map<String, Object>> getOrderDetails(String userAccount) {
        return orderDetailsMapper.getOrderDetails(userAccount);
    }
}