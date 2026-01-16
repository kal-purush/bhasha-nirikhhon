package com.kodilla.ecommerce.domain;

import com.kodilla.ecommerce.domain.enums.StatusUser;
import com.kodilla.ecommerce.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class UserTestSuite {
    @Autowired
    private UserRepository userRepository;
    private final Order orderTest = new Order();
    private final List<Order> orderListTest = new ArrayList<>();
    private final User userTest = new User();
    private final Cart cartTest = new Cart();

    @Test
    void testSaveUser() {
        //Given
        orderTest.setNumber("123");

        orderListTest.add(orderTest);

        cartTest.setUser(userTest);

        userTest.setName("Test - user");
        userTest.setStatus(StatusUser.ACTIVE_USER);
        userTest.setUserKey(1000L);
        userTest.setOrders(orderListTest);
        userTest.setCart(cartTest);
        //When
        userRepository.save(userTest);
        Long userTestId = userTest.getId();
        //Then
        Optional<User> readUserId = userRepository.findById(userTestId);
        assertTrue(readUserId.isPresent());
        assertEquals("Test - user", userTest.getName());
        assertEquals(StatusUser.ACTIVE_USER, userTest.getStatus());
        assertEquals(1000L, userTest.getUserKey());
        assertEquals(1, userTest.getOrders().size());
        assertNotNull(userTest.getCart());
        //CleanUp
        userRepository.deleteById(userTestId);
    }

    @Test
    void testBlockUser() {
        //Given
        orderTest.setNumber("123");

        orderListTest.add(orderTest);

        cartTest.setUser(userTest);

        userTest.setName("Test - user");
        userTest.setStatus(StatusUser.ACTIVE_USER);
        userTest.setUserKey(1000L);
        userTest.setOrders(orderListTest);
        userTest.setCart(cartTest);

        userRepository.save(userTest);
        //When
        userTest.setStatus(StatusUser.BLOCKED_USER);
        userRepository.save(userTest);
        //Then
        Long userTestId = userTest.getId();
        Optional<User> readUserId = userRepository.findById(userTestId);
        assertTrue(readUserId.isPresent());
        assertNotEquals(StatusUser.ACTIVE_USER, userTest.getStatus());
        //CleanUp
        userRepository.deleteById(userTestId);
    }

    @Test
    void testGenerateUserKey() {
        //Given
        orderTest.setNumber("123");

        orderListTest.add(orderTest);

        cartTest.setUser(userTest);

        userTest.setName("Test - user");
        userTest.setStatus(StatusUser.ACTIVE_USER);
        userTest.setUserKey(null);
        userTest.setOrders(orderListTest);
        userTest.setCart(cartTest);

        userRepository.save(userTest);
        //When
        userTest.setUserKey(1000L);
        userRepository.save(userTest);
        //Then
        Long userTestId = userTest.getId();
        Optional<User> readUserId = userRepository.findById(userTestId);
        assertTrue(readUserId.isPresent());
        assertNotNull(userTest.getUserKey());
        //CleanUp
        userRepository.deleteById(userTestId);
    }
}