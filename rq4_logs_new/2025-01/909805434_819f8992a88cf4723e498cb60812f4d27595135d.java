package coffee.order;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class CoffeeOrderBoard {
    private List<Order> orders;
    private int orderCounter;

    public CoffeeOrderBoard() {
        this.orders = new ArrayList<>();
        this.orderCounter = 100;
    }

    public void add(String customerName) {
        Order order = new Order(orderCounter++,customerName);
        orders.add(order);
    }

    public List<Order> getOrders() {
        return orders;
    }

    public Order deliver() {
        if(orders.isEmpty()) {
            return null;
        }
        Order nearestOrder = orders.get(0);
        for(Order order : orders) {
            if(order.getOrderNumber() < nearestOrder.getOrderNumber()) {
                nearestOrder = order;
            }
        }
        orders.remove(nearestOrder);
        return nearestOrder;
    }

    public Order deliver(int orderNumber) {
        for(Order order : orders) {
            if(order.getOrderNumber() == orderNumber) {
                orders.remove(order);
                return order;
            }
        }
        return null;
    }

    public void draw () {
        System.out.println("Number | Customer");
        for(Order order : orders) {
            System.out.println(order.getOrderNumber() + " | " + order.getCustomerName());
        }
    }

}
