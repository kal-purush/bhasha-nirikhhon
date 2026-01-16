package com.internet.shop;

import com.internet.shop.lib.Injector;
import com.internet.shop.model.Product;
import com.internet.shop.service.ProductService;

public class Application {
    private static Injector injector = Injector.getInstance("com.internet.shop");

    public static void main(String[] args) {
        ProductService productService = (ProductService) injector.getInstance(ProductService.class);

        productService.create(new Product("Iphone 10", 1_200));
        productService.create(new Product("Iphone 11", 1_400));
        productService.create(new Product("Iphone X", 1_600));

        System.out.println(productService.getAll());
        productService.deleteById(2L);
        System.out.println(productService.getAll());
        Product newIphone = new Product("Iphone 12",1800);
        newIphone.setId(3L);
        productService.update(newIphone);
        System.out.println(productService.getAll());
        System.out.println(productService.getById(1L));
    }
}