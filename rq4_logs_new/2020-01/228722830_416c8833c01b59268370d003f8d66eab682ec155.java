package com.kodilla.ecommercee.repository;

import com.kodilla.ecommercee.domain.Product;
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
public class ProdutRepositoryTestSuite {
    @Autowired
    private ProductRepository productRepository;

    @Test
    public void testSaveProduct() {
        //Given
        Product product = new Product();
        //When
        productRepository.save(product);
        Long productId = product.getId();
        //Then
        assertNotEquals(0, (long) productId);
    }

    @Test
    public void testFindAllProducts() {
        //Given
        Product product = new Product("Weed");
        Product productOne = new Product("Coke");
        productRepository.save(product);
        productRepository.save(productOne);
        //When
        List<Product> products = productRepository.findAll();
        //Then
        assertEquals(2, products.size());
    }

    @Test
    public void testFindProductById() {
        //Given
        Product product = new Product("Weed");
        Product productOne = new Product("Coke");
        productRepository.save(product);
        productRepository.save(productOne);
        Long productId = product.getId();
        //When
        Optional<Product> productOptional = productRepository.findById(productId);
        //Then
        assertTrue(productOptional.isPresent());
    }

    @Test
    public void testDeleteProductById() {
        //Given
        Product product = new Product("Weed");
        Product productOne = new Product("Coke");
        productRepository.save(product);
        productRepository.save(productOne);
        Long productId = product.getId();
        //When
        productRepository.deleteById(productId);
        Optional<Product> productOptional = productRepository.findById(productId);
        //Then
        assertFalse(productOptional.isPresent());
    }
}