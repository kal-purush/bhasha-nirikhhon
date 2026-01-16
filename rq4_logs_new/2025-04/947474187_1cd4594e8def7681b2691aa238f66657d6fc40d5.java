package com.appsdeveloperblog.core.events;

import java.util.UUID;

public class ProductReservationFailedEvent {

    private UUID orderId;
    private UUID productId;
    private Integer productQuantity;

    public ProductReservationFailedEvent() {
    }

    public ProductReservationFailedEvent(UUID orderId, UUID productId, Integer productQuantity) {
        this.orderId = orderId;
        this.productId = productId;
        this.productQuantity = productQuantity;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

    public Integer getProductQuantity() {
        return productQuantity;
    }

    public void setProductQuantity(Integer productQuantity) {
        this.productQuantity = productQuantity;
    }

}