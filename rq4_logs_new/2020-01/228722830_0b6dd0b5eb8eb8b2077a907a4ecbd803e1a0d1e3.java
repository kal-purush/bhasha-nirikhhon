package com.kodilla.ecommercee.repository;

import com.kodilla.ecommercee.domain.Cart;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class CartRepositoryTestSuite {
    @Autowired
    private CartRepository cartRepository;

    @Test
    public void testSaveCart() {
        //Given
        Cart cart = new Cart();
        //When
        cartRepository.save(cart);
        long cartId = cart.getId();
        //Then
        assertNotEquals(0, cartId);
    }

    @Test
    public void testFindAllCarts() {
        //Given
        Cart cartOne = new Cart();
        Cart cartTwo = new Cart();
        cartRepository.save(cartOne);
        cartRepository.save(cartTwo);
        //When
        List<Cart> carts = cartRepository.findAll();
        //Then
        assertEquals(2, carts.size());
    }

    @Test
    public void testFindCartById() {
        //Given
        Cart cartOne = new Cart();
        Cart cartTwo = new Cart();
        cartRepository.save(cartOne);
        cartRepository.save(cartTwo);
        Long cartId = cartOne.getId();
        //When
        Optional<Cart> cartOptional = cartRepository.findById(cartId);
        //Then
        assertTrue(cartOptional.isPresent());
    }

    @Test
    public void testDeleteCartById() {
        //Given
        Cart cartOne = new Cart();
        Cart cartTwo = new Cart();
        cartRepository.save(cartOne);
        cartRepository.save(cartTwo);
        Long cartId = cartOne.getId();
        //When
        cartRepository.deleteById(cartId);
        Optional<Cart> cartOptional = cartRepository.findById(cartId);
        //Then
        assertFalse(cartOptional.isPresent());
    }

}