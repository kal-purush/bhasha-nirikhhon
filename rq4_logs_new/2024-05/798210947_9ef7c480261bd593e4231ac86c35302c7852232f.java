package com.punnyajoshi.Ecommerce;

import com.punnyajoshi.Ecommerce.Pages.Product;

public class ShoppingCartApp {
    public static void main(String[] args) {
        // Create cart instance
        Cart cart = new Cart();

        // Adding products to the cart (example products)
        cart.addProduct(new Product("T-Shirt", "M", 2, 19.99));
        cart.addProduct(new Product("Jeans", "32", 1, 39.99));

        // Logic to display cart contents
        for (Product product : cart.getProducts()) {
            System.out.println(product);
        }

        // Compute and show total price here
        System.out.println("Total price: $" + cart.getTotalPrice());
    }
}