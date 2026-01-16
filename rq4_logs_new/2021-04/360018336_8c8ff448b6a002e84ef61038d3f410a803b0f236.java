package ru.geekbrains.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Client {
    public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext("config.xml");
        ProductRepository cart = context.getBean(ProductRepository.class);

        System.out.println(cart.getProducts().toString());

        cart.addProduct("Pumpkin", 5);
        cart.deleteProduct(15);

        System.out.println(cart.getProduct(6));

    }
}