package com.kodilla.good.patterns.food2door;

import com.kodilla.good.patterns.food2door.order.Order;
import com.kodilla.good.patterns.food2door.order.OrderExecutor;
import com.kodilla.good.patterns.food2door.order.OrderRetriever;
import com.kodilla.good.patterns.food2door.process.HealthyShop;

public class Food2DoorMain {

    public static void main(String[] args) {

        OrderRetriever orderRetriever = new OrderRetriever();
        Order order = orderRetriever.retrieve();

        OrderExecutor orderExecutor = new OrderExecutor(new HealthyShop());
        orderExecutor.execute(order);
    }

}