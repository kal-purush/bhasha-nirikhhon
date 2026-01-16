package com.kodilla.ecommerce.domain;


import com.kodilla.ecommerce.repository.ProductRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class ProductTest {

    @Autowired
    private ProductRepository productRepository;

    @Test
    public void testSaveProduct() {
        //Given
        Product productTest = new Product();
        Group groupTest = new Group();

        productTest.setName("Save Product Test");
        productTest.setPrice(new BigDecimal(16));
        productTest.setDescription("Test Description");
        productTest.setGroup(groupTest);

        groupTest.setName("Test Group Name");

        //When
        productRepository.save(productTest);


        //Then
        Long productTestId= productTest.getId();
        Optional<Product> testProductId = productRepository.findById(productTestId);
        List<Product> testProduct = productRepository.findAll();
        assertTrue(testProductId.isPresent());
        assertEquals("Save Product Test",testProduct.get(0).getName());
        assertEquals("Test Description",testProduct.get(0).getDescription());
        assertEquals(new BigDecimal("16.00"), testProduct.get(0).getPrice());
        assertEquals(groupTest, testProduct.get(0).getGroup());
        assertEquals("Test Group Name", testProduct.get(0).getGroup().getName());
        //CleanUp
        productRepository.deleteById(productTestId);
    }

    @Test
    public void testUpdateProduct() {
        //Given
        Product productTest = new Product();
        Group groupTest = new Group();

        productTest.setName("Update Product Test");
        productTest.setPrice(new BigDecimal(16));
        productTest.setDescription("Test Description");
        productTest.setGroup(groupTest);
        groupTest.setName("Test Group Name");

        productRepository.save(productTest);

        //When
        productTest.setName("Update");
        productRepository.save(productTest);

        //Then
        Long productTestId = productTest.getId();
        Optional<Product> testProductId = productRepository.findById(productTestId);
        List<Product> testProduct = productRepository.findAll();
        assertTrue(testProductId.isPresent());
        assertEquals("Update",testProduct.get(0).getName());
        assertEquals("Test Description",testProduct.get(0).getDescription());
        assertEquals(new BigDecimal("16.00"), testProduct.get(0).getPrice());
        assertEquals(groupTest, testProduct.get(0).getGroup());
        //CleanUp
        productRepository.deleteById(productTestId);

    }

    @Test
    public void testDeleteProduct() {
        //Given
        Product productTest = new Product();
        Group groupTest = new Group();

        productTest.setName("Test Product Delete");
        productTest.setPrice(new BigDecimal(16));
        productTest.setDescription("Test Description");
        productTest.setGroup(groupTest);
        groupTest.setName("Test Group Name");

        productRepository.save(productTest);

        //When
        Long productTestId = productTest.getId();
        productRepository.deleteById(productTestId);
        Optional<Product> readProduct = productRepository.findById(productTestId);

        //Then
        assertFalse(readProduct.isPresent());
    }
}