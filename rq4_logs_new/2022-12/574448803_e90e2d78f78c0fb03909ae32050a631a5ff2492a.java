package KitchenSystem.MicroService.application.dto;

import KitchenSystem.MicroService.domain.Order;
import KitchenSystem.MicroService.domain.Status;

import java.util.List;

public class OrderData {
    public final Long orderId;
    public final int tableNumber;
    public final Status status;

    public OrderData(Long orderId, int tableNumber, Status status) {
        this.orderId = orderId;
        this.tableNumber = tableNumber;
        this.status = status;
    }
}