package ru.inno;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.inno.service.OrdersService;
import ru.inno.service.UsersService;

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class Main {

    private final UsersService usersService;
    private final OrdersService ordersService;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @PostConstruct
    public void init() {
        var users = usersService.getUsers();
        log.info("Все пользователи: {}}", users);
        var userId = users.getFirst().getId();
        var user = usersService.getUser(userId);
        log.info("Пользователь ID={}: {}", userId, user);
        var orders = ordersService.getOrders();
        log.info("Все заказы: {}", orders);
        var orderId = orders.getFirst().getId();
        var order = ordersService.getOrder(orderId);
        log.info("Заказ ID={}: {}", orderId, order);
        ordersService.deleteAllOrders();
        orders = ordersService.getOrders();
        log.info("Все заказы: {}", orders);
        usersService.deleteAllUsers();
        users = usersService.getUsers();
        log.info("Все пользователи: {}}", users);
    }

}
