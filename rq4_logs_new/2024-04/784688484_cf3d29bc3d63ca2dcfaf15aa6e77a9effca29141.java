package ru.task.deliveryapp.core.application.usecases.commands;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import ru.task.deliveryapp.core.ports.CourierRepository;
import ru.task.deliveryapp.core.ports.OrderRepository;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class AssignOrdersHandlerIT {
    @Autowired
    AssignOrdersHandler handler;

    @Autowired
    CourierRepository courierRepository;

    @Autowired
    OrderRepository orderRepository;

    @Test
    @Sql("/uc_assign_orders_data.sql")
    public void testHandle() {
        handler.handle();
        var listReadyCouriers = courierRepository.getAllReady();
        var listBusyCouriers = courierRepository.getAllBusy();
        var listOrdersNotAssigned = orderRepository.getAllNotAssigned();
        var listOrdersAssigned = orderRepository.getAllAssigned();
        assertAll("Testing AssignOrders use case",
                () -> assertEquals(0, listReadyCouriers.size()),
                () -> assertEquals(4, listBusyCouriers.size()),
                () -> assertEquals(1, listOrdersNotAssigned.size()),
                () -> assertEquals(UUID.fromString("f46b347e-198f-403c-a04d-a1d00e5632be"), listOrdersNotAssigned.get(0).getId()),
                () -> assertEquals(UUID.fromString("303c5141-7c46-4836-ab3c-027e1cbe075a"), listOrdersAssigned.get(0).getId()),
                () -> assertEquals(UUID.fromString("0de247d3-3de7-45d5-98eb-114c721ff6d8"), listOrdersAssigned.get(0).getCourierId()),
                () -> assertEquals(UUID.fromString("3e7d532f-6a53-406f-829a-b0c9f6a3208a"), listOrdersAssigned.get(1).getId()),
                () -> assertEquals(UUID.fromString("407f68be-5adf-4e72-81bc-b1d8e9574cf8"), listOrdersAssigned.get(1).getCourierId()),
                () -> assertEquals(UUID.fromString("f1df1fd4-228e-46f3-ba04-2661583da754"), listOrdersAssigned.get(2).getId()),
                () -> assertEquals(UUID.fromString("407adabd-dc4e-405b-9ee9-c324154a36c4"), listOrdersAssigned.get(2).getCourierId()),
                () -> assertEquals(UUID.fromString("f2959d3e-aadd-470c-9530-c60f3843300a"), listOrdersAssigned.get(3).getId()),
                () -> assertEquals(UUID.fromString("bf79a004-56d7-4e5f-a21c-0a9e5e08d10d"), listOrdersAssigned.get(3).getCourierId()),
                () -> assertEquals(4, listOrdersAssigned.size())
        );
    }
}