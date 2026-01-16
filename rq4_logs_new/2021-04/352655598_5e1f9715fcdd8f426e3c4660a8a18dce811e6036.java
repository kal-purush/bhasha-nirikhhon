package com.kodilla.ecommerce.domain;

import com.kodilla.ecommerce.domain.enums.StatusUser;
import com.kodilla.ecommerce.repository.CartItemRepository;
import com.kodilla.ecommerce.repository.CartRepository;
import com.kodilla.ecommerce.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.transaction.Transactional;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Transactional
@ExtendWith(SpringExtension.class)
@SpringBootTest
public class CartTestSuite {

    @Autowired
    private CartRepository cartRepository;

    @Test
    public void testSaveCart(){
        // Given
        Cart cartTest = new Cart();

        User userTest =User.builder()
                .cart(cartTest)
                .name("User Test")
                .status(StatusUser.ACTIVE_USER)
                .userKey(59875L)
                .build();

        cartTest.setUser(userTest);


        // When
        cartRepository.save(cartTest);
        Long cartTestId = cartTest.getId();
        System.out.println(cartTestId);
        Optional<Cart> cartTestById = cartRepository.findById(cartTestId);

        // Then
        assertTrue(cartRepository.existsById(cartTestId));
        assertTrue(cartTestById.isPresent());

        // Clean-up
        cartRepository.deleteById(cartTestId);
    }

    @Test
    public void testUpdateCart(){
        // Given
        Cart cartTest = new Cart();

        User userTest =User.builder()
                .cart(cartTest)
                .name("User Test")
                .status(StatusUser.ACTIVE_USER)
                .userKey(59875L)
                .build();

        cartTest.setUser(userTest);

        // When
        cartRepository.save(cartTest);
        Long cartTestId = cartTest.getId();
        CartItem cartItemTest = new CartItem();
        cartTest.getItems().add(cartItemTest);
        cartRepository.save(cartTest);
        Optional<Cart> cartTestById = cartRepository.findById(cartTestId);

        // Then
        assertTrue(cartRepository.existsById(cartTestId));
        assertTrue(cartTestById.isPresent());
        assertTrue(cartTest.getItems().contains(cartItemTest));

        // Clean-up
        cartRepository.deleteById(cartTestId);
    }

    @Test
    public void testDeleteCart(){
        // Given
        Cart cartTest = new Cart();

        User userTest =User.builder()
                .cart(cartTest)
                .name("User Test")
                .status(StatusUser.ACTIVE_USER)
                .userKey(59875L)
                .build();

        cartTest.setUser(userTest);

        // When
        cartRepository.save(cartTest);
        Long cartTestId = cartTest.getId();
        cartRepository.deleteById(cartTestId);
        Optional<Cart> cartTestById = cartRepository.findById(cartTestId);

        // Then
        assertFalse(cartRepository.existsById(cartTestId));
        assertTrue(cartTestById.isEmpty());
    }
}