package org.skypro.skyshop;

import org.skypro.skyshop.basket.ProductBasket;
import org.skypro.skyshop.product.Product;

public class App {
    public static void main(String[] args) {
        ProductBasket basket = new ProductBasket();

        Product apple = new Product("Яблоки", 250);
        Product bananas = new Product("Бананы", 170);
        Product sugar = new Product("Сахар", 120);
        Product milk = new Product("Молоко", 80);
        Product mango = new Product("Манго", 300);
        Product lime = new Product("Лайм", 50);

        System.out.println("Добавили продукт в корзину");
        basket.addProduct(apple);
        basket.printCartContents();

        System.out.println("Заполнение корзины");
        basket.addProduct(bananas);
        basket.addProduct(sugar);
        basket.addProduct(milk);
        basket.addProduct(mango);
        basket.printCartContents();

        System.out.println("Добавление продукта в заполненную корзину, в которой нет свободного места");
        basket.addProduct(lime);

        System.out.println("Печать содержимого корзины с несколькими товарами");
        basket.printCartContents();

        System.out.println("Получение стоимости корзины с несколькими товарами");
        System.out.println("Стоимость корзины с продуктами " + basket.getTotalAmount() + " рублей");

        System.out.println("Поиск товара, который есть в корзине");
        System.out.println("Манго в корзине? " + basket.containsProduct("Манго"));

        System.out.println("Поиск товара, которого нет в корзине");
        System.out.println("Батон в корзине? " + basket.containsProduct("Батон"));

        System.out.println("Очистка корзины");
        basket.clear();

        System.out.println("Печать содержимого пустой корзины");
        basket.printCartContents();

        System.out.println("Получение стоимости пустой корзины");
        System.out.println("Стоимость пустой корзины " + basket.getTotalAmount() + " рублей");

        System.out.println("Поиск Манго в пустой корзине");
        System.out.println("Манго в корзине? " + basket.containsProduct("Манго"));

    }

}