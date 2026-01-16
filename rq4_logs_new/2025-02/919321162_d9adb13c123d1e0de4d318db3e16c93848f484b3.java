package com.fptgang.backend.service.impl;

import com.fptgang.backend.model.Order;
import com.fptgang.backend.model.OrderDetail;
import com.fptgang.backend.repository.AccountRepos;
import com.fptgang.backend.repository.OrderDetailRepos;
import com.fptgang.backend.repository.OrderRepos;
import com.fptgang.backend.service.StatService;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class StatServiceImpl implements StatService {


    private final OrderRepos orderRepos;
    private final AccountRepos accountRepos;

    public StatServiceImpl(OrderRepos orderRepos, AccountRepos accountRepos) {
        this.orderRepos = orderRepos;
        this.accountRepos = accountRepos;
    }

    @Override
    public List<Map<String, Object>> getDailyOrders() {
        return orderRepos.getDailyOrders().stream()
                .map(entry -> Map.of(
                        "x", (Object) entry[0].toString(),  // Ensure it's Object type
                        "y", (Object) ((Number) entry[1]).intValue() // Convert to Integer
                ))
                .collect(Collectors.toList());
    }

    @Override
    public List<Map<String, Object>> getDailyRevenue() {
        return orderRepos.getDailyRevenue().stream()
                .map(entry -> Map.of(
                        "x", (Object) entry[0].toString(),  // Ensure it's Object type
                        "y", (Object) ((Number) entry[1]).doubleValue() // Convert to Double
                ))
                .collect(Collectors.toList());
    }

    @Override
    public List<Map<String, Object>> getMonthlyRevenue() {
        return orderRepos.getMonthlyRevenue().stream()
                .map(entry -> Map.of(
                        "x", (Object) entry[0].toString(),
                        "y", (Object) ((Number) entry[1]).doubleValue()
                ))
                .collect(Collectors.toList());
    }

    @Override
    public List<Map<String, Object>> getDailyNewCustomers() {
        return accountRepos.getDailyNewCustomers().stream()
                .map(entry -> Map.<String, Object>of( // Explicit type specification
                        "x", entry[0] != null ? entry[0].toString() : "Unknown Date", // Ensure x-axis (date) is a String
                        "y", entry[1] != null ? ((Number) entry[1]).intValue() : 0  // Convert count to Integer, default to 0
                ))
                .collect(Collectors.toList());
    }

    @Override
    public List<Map<String, Object>> getMonthlyNewCustomers() {
        return accountRepos.getMonthlyNewCustomers().stream()
                .map(entry -> Map.<String, Object>of( // Explicit type specification
                        "x", entry[0] != null ? entry[0].toString() : "Unknown Month", // Ensure month is a String
                        "y", entry[1] != null ? ((Number) entry[1]).intValue() : 0  // Convert count to Integer, default to 0
                ))
                .collect(Collectors.toList());
    }
}
