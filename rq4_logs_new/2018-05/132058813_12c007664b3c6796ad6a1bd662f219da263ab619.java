package by.ras.repository;

import by.ras.BaseTest;
import by.ras.entity.OrderStatus;
import by.ras.entity.particular.Order;
import lombok.extern.log4j.Log4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;
import java.util.LinkedList;
import java.util.List;

import static by.ras.repository.specification.OrderSpecification.searchOrders;
import static org.junit.Assert.*;

@Log4j
@Transactional
public class OrderRepositoryTest extends BaseTest {

    @Autowired
    OrderRepository orderRepository;

    @Test
    public void findAllPredicat() throws Exception {
        List<Order> ordersExpected = fillTableOrders();
        Order filter = new Order();
        List<Order> orders = orderRepository.findAll(searchOrders(filter));
        assertEquals(ordersExpected, orders);

    }

    private List<Order> fillTableOrders() {
        String[] statuses = {OrderStatus.NEW.name(), OrderStatus.ACTIVE.name(), OrderStatus.CLOSED.name()};
        List<Order> orders = new LinkedList<>();
        for (int i = 0; i < statuses.length; i++) {
            for (int k = 0; k < 5; k++) {
                Order order = Order.builder()
                        .date(new Date(System.currentTimeMillis()))
                        .totalSum(String.valueOf((i + 1) * k))
                        .orderStatus(statuses[i])
                        .build();
                orders.add(order);
                orderRepository.saveAndFlush(order);
            }
        }
        return orders;
    }

}