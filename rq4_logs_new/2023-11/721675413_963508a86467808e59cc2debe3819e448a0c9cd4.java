package com.codescouts.hexagonal.codescoutshexagonaltraining.order.domain.commands;

import java.util.List;
import java.util.UUID;

public record CheckoutOrder(UUID customerId, List<OrderProductLine> items, String currencyCode) {


}