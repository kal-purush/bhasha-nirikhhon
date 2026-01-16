package stationary.store.utilities.json;

import stationary.store.model.Address;
import stationary.store.model.OrderItem;
import java.util.List;

public class OrderDetail {
    private List<OrderItem> orderItems;
    private Address address;

    public List<OrderItem> getOrderItems() {
        return orderItems;
    }

    public void setOrderItems(List<OrderItem> orderItems) {
        this.orderItems = orderItems;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }
}