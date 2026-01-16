package com.zenith.notification_service.events;


import com.zenith.notification_service.models.enums.OrderStatus;

public record OrderEvent(String orderNumber, int itemsCount, OrderStatus orderStatus) {
}