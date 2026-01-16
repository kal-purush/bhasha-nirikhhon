package com.appsdeveloperblog.core.dto.commands;

import java.util.UUID;

public class CancelProductReservationCommand {

    private UUID productId;
    private UUID orderId;
    private Integer pruductQuantity;

    public CancelProductReservationCommand() {
    }

    public CancelProductReservationCommand(UUID productId, UUID orderId, Integer pruductQuantity) {
        this.productId = productId;
        this.orderId = orderId;
        this.pruductQuantity = pruductQuantity;
    }

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    public Integer getPruductQuantity() {
        return pruductQuantity;
    }

    public void setPruductQuantity(Integer pruductQuantity) {
        this.pruductQuantity = pruductQuantity;
    }

}