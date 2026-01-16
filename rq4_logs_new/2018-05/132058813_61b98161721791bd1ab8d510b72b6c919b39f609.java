package by.ras.impl;

import by.ras.BaseTest;
import by.ras.OrderService;
import by.ras.entity.Occupation;
import by.ras.entity.OrderStatus;
import by.ras.entity.Role;
import by.ras.entity.Sex;
import by.ras.entity.particular.Order;
import by.ras.entity.particular.User;
import by.ras.repository.OrderRepository;
import by.ras.repository.UserRepository;
import lombok.extern.log4j.Log4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

@Log4j
@Transactional
public class OrderServiceTest extends BaseTest {

    @Autowired
    OrderRepository orderRepository;
    @Autowired
    OrderService orderService;
    @Autowired
    UserRepository userRepository;

    @Test
    public void addOrder() throws Exception {
        User user = userRepository.findByLogin("user");
        Order order = new Order();
        order.setUser(user);
        order.setOrderStatus(OrderStatus.CLOSED.name());
        order.setTotalSum("1111");
        order = orderService.addOrder(order);
        Order foundOrder = userRepository.findByLogin("user").getOrders().get(0);
        assertEquals(order, foundOrder);
    }

    @Test
    public void findById() throws Exception {
        User user = userRepository.findByLogin("user");
        Order order = new Order();
        order.setUser(user);
        order.setOrderStatus(OrderStatus.CLOSED.name());
        order.setTotalSum("1111");
        order = orderService.addOrder(order);
        Order foundOrder = orderService.findById(order.getId());
        assertEquals(order, foundOrder);
    }

    @Test
    public void findAll() throws Exception {
        int objPerPage = 4;
        User user = userRepository.findByLogin("user");
        List<Order> orders = new LinkedList<>();
        for(int i = 0; i < 10; i++) {
            Order order = new Order();
            order.setUser(user);
            order.setOrderStatus(OrderStatus.CLOSED.name());
            order.setTotalSum("1111");
            order = orderService.addOrder(order);
            if(i < objPerPage){
                orders.add(order);
            }
        }
        List<Order> foundOrders = orderService.findAll(new PageRequest(0, objPerPage));
        assertEquals(orders, foundOrders);
    }

    @Test
    public void findAllComplex() throws Exception {
        int objPerPage = 15;
        Order filter = null;
        User user = userRepository.findByLogin("user");
        List<Order> orders = new LinkedList<>();
        for(int i = 0; i < 10; i++) {
            Order o = new Order();
            o.setUser(user);
            o.setTotalSum("1111");
            o = orderService.addOrder(o);
            if(i < objPerPage){
                orders.add(o);
                filter = o;
            }
        }
        for(int i = 0; i < 10; i++) {
            Order o = new Order();
            o.setUser(user);
            o.setOrderStatus(OrderStatus.CLOSED.name());
            o.setTotalSum("1111");
            o = orderRepository.saveAndFlush(o);
        }
        List<Order> foundOrders = orderService.findAllComplex(filter, new PageRequest(0, objPerPage));
        assertEquals(orders, foundOrders);
    }

    @Test
    public void editOrder() throws Exception {
        User user = userRepository.findByLogin("user");
        Order o = new Order();
        o.setUser(user);
        o.setOrderStatus(OrderStatus.CLOSED.name());
        o.setTotalSum("1111");
        o = orderRepository.saveAndFlush(o);
        Order order = userRepository.findByLogin("user").getOrders().get(0);
        order.setTotalSum("2222");
        orderService.editOrder(order);
        Order foundOrder = userRepository.findByLogin("user").getOrders().get(0);
        assertEquals(order, foundOrder);

    }

    @Test
    public void countRows() throws Exception {
        User user = userRepository.findByLogin("user");
        List<Order> orders = new LinkedList<>();
        for(int i = 0; i < 10; i++) {
            Order order = new Order();
            order.setUser(user);
            order.setOrderStatus(OrderStatus.CLOSED.name());
            order.setTotalSum("1111");
            order = orderService.addOrder(order);
            orders.add(order);

        }
        long foundRows = orderService.countRows();
        assertEquals(orders.size(), foundRows);
    }

    @Test
    public void countRowsComplex() throws Exception {
        int objPerPage = 15;
        Order filter = null;
        User user = userRepository.findByLogin("user");
        List<Order> orders = new LinkedList<>();
        for(int i = 0; i < 10; i++) {
            Order o = new Order();
            o.setUser(user);
            o.setTotalSum("1111");
            o = orderService.addOrder(o);
            if(i < objPerPage){
                orders.add(o);
                filter = o;
            }
        }
        for(int i = 0; i < 10; i++) {
            Order o = new Order();
            o.setUser(user);
            o.setOrderStatus(OrderStatus.CLOSED.name());
            o.setTotalSum("1111");
            o = orderRepository.saveAndFlush(o);
        }
        long foundRows = orderService.countRowsComplex(filter);
        assertEquals(orders.size(), foundRows);
    }

}