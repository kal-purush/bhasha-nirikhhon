package ucu.edu.apps.flowerstore;

import lombok.Getter;
import ucu.edu.apps.flowerstore.delivery.Delivery;
import ucu.edu.apps.flowerstore.items.Item;
import ucu.edu.apps.flowerstore.payment.Payment;

import java.util.LinkedList;
import java.util.List;

public class Order {
    @Getter
    private List<Item> items = new LinkedList<>();
    private Payment payment;
    private Delivery delivery;

    public void setPaymentStrategy(Payment payment) {
        this.payment = payment;
    }

    public void setDeliveryStrategy(Delivery delivery) {
        this.delivery = delivery;
    }

    public double calculateTotalPrice() {
        double price = 0;
        for (Item item: items) {
            price += item.getPrice();
        }
        return price;
    }

    public void processOrder() {
        System.out.println("Process started");
    }

    public void addItem(Item item) {
        items.add(item);
    }

    public void removeItem(Item item) {
        this.items.remove(item);
    }
}