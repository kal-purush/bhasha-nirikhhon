package com.kodilla.ecommercee.domain;

import com.kodilla.ecommercee.repository.CartRepository;
import com.kodilla.ecommercee.repository.OrderRepository;
import com.kodilla.ecommercee.repository.UserRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.transaction.Transactional;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class CartRepositoryTestSuite {

    @Autowired
    CartRepository cartRepository;

    @Autowired
    UserRepository userRepository;

    @Autowired
    OrderRepository orderRepository;

    private Cart cart;
    private User user;
    private Order order;

    private void createData() {
        user = new User(0, "test user", false, new UserKey(), new ArrayList<>(), new ArrayList<>());
        cart = new Cart(0, new ArrayList<>(), user);
    }

    private void deleteData() {
            userRepository.delete(user);
            cartRepository.delete(cart);
    }

    private void prepareDataAndSave() {
        user = new User(0, "test user", false, new UserKey(), new ArrayList<>(), new ArrayList<>());
        cart = new Cart(0, new ArrayList<>(), user);
        user.addCart(cart);
        userRepository.save(user);
        cartRepository.save(cart);
    }

    @Test
    public void testCartRepositorySave() {
        //Given
        createData();

        //When
        cartRepository.save(cart);

        //Then
        assertNotEquals(0, cart.getId());

        //CleanUp
        deleteData();
    }

    @Test
    public void testCartRepositoryGet() {
        //Given
        prepareDataAndSave();

        //When
        Optional<Cart> result = cartRepository.findById(cart.getId());

        //Then
        assertTrue(result.isPresent());
        assertEquals("test user", result.orElse(new Cart()).getUser().getUsername());

        //CleanUp
        deleteData();
    }

    @Test
    public void testCartRepositoryUpdate() {
        //Given
        prepareDataAndSave();

        //When
        User newUser = new User(0, "new User", user.isBlocked(), user.getUserKey(), user.getCarts(), user.getOrders());
        userRepository.save(newUser);
        cart.setUser(newUser);
        cartRepository.save(cart);

        //Then
        assertEquals(cart.getUser().getId(), newUser.getId());

        //CleanUp
        deleteData();
    }

    @Test
    public void testCreateOrder() {
        //Given
        prepareDataAndSave();

        //When
        User user = userRepository.findById(this.user.getId()).orElse(null);
        Cart cart = cartRepository.findById(this.cart.getId()).orElse(null);
        order = new Order(0, user, cart);
        orderRepository.save(order);
        Optional<Order> resultById = orderRepository.findById(order.getId());

        //Then
        assertEquals(resultById.get().getId(), order.getId());

        //CleanUp
        deleteData();
    }

    @Test
    public void testCartRepositoryDelete() {
        //Given
       prepareDataAndSave();

       //When
        cartRepository.deleteById(cart.getId());
        Optional<Cart> resultById = cartRepository.findById(cart.getId());
        List<Cart> result = cartRepository.findAll();

        //Then
        assertFalse(resultById.isPresent());
        assertEquals(0,result.size());
    }


}