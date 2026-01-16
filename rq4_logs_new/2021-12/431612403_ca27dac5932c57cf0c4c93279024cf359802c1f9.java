package com.example.demo.controllers;

import com.example.demo.models.Product;
import com.example.demo.services.ProductServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
@Slf4j
class ShopControllerImplTest {

    ProductServiceImpl productService;

    @Autowired
    public ShopControllerImplTest(ProductServiceImpl productService) {
        this.productService = productService;
    }

    @Test
    void testSaveAndDeleteProduct() {
        Product product = createProduct();
        productService.saveProduct(product);
        log.info(productService.findAllProducts().toString());
        Assertions.assertEquals(productService.findAllProducts(), List.of(product));

        productService.deleteProduct(product);
        Assertions.assertTrue(productService.findAllProducts().isEmpty());
    }

    @Test
    void testFindProductById() {
        Product product = createProduct();
        productService.saveProduct(product);

        Assertions.assertEquals(productService.findProduct(product.getId()).get(), product);

        productService.deleteProduct(product);
        Assertions.assertTrue(productService.findAllProducts().isEmpty());
    }

    @Test
    void testFindProductByType() {
        Product product = createProduct();
        productService.saveProduct(product);

        Assertions.assertEquals(productService.findProductByType(product.getType()).get(0), product);

        productService.deleteProduct(product);
        Assertions.assertTrue(productService.findAllProducts().isEmpty());
    }

    @Test
    void testFindAllProductByName() {
        Product product = createProduct();
        productService.saveProduct(product);

        Assertions.assertEquals(productService.findAllProductByName(product.getName()).get(0), product);

        productService.deleteProduct(product);
        Assertions.assertTrue(productService.findAllProducts().isEmpty());
    }

    @Test
    void testFindAllProductByCost() {
        Product product = createProduct();
        productService.saveProduct(product);

        Assertions.assertEquals(productService.findAllProductByCost(product.getCost()).get(0), product);

        productService.deleteProduct(product);
        Assertions.assertTrue(productService.findAllProducts().isEmpty());
    }

    Product createProduct() {
        return new Product("Name.png", "Name", "Type", "Desc", 20);
    }
}