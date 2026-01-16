package org.shoppingcart;

import org.shoppingcart.cart.ShoppingCart;
import org.shoppingcart.config.ProductConfigurationLoader;
import org.shoppingcart.product.ProductFactory;
import java.util.*;

public class Main {
    public static void main(String[] args) {

        ProductConfigurationLoader loader = new ProductConfigurationLoader("src/main/resources/product_config.properties", "src/main/resources/pricing_config.properties");
        ProductFactory productFactory = loader.loadProductFactory();
        ShoppingCart shoppingCart = new ShoppingCart();

        /* Another Option
        List<String> productNames = Arrays.asList("apple", "banana", "melon", "melon", "lime", "lime", "lime");
        Map<String, Integer> productQuantities = new HashMap<>();

        for (String productName : productNames) {
            if (!productQuantities.containsKey(productName)) {
                productQuantities.put(productName, 1);
            } else {
                int quantity = productQuantities.get(productName);
                productQuantities.put(productName, quantity + 1);
            }
        }



        for (Map.Entry<String, Integer> entry : productQuantities.entrySet()) {
            String productName = entry.getKey();
            int quantity = entry.getValue();
            shoppingCart.addItem(productFactory.getProduct(productName), quantity);
        }

         */


        shoppingCart.addItem(productFactory.getProduct("apple"), 1);
        shoppingCart.addItem(productFactory.getProduct("banana"), 1);
        shoppingCart.addItem(productFactory.getProduct("melon"), 2);
        shoppingCart.addItem(productFactory.getProduct("lime"), 3);

        System.out.println("Total: " + shoppingCart.getTotal());
    }
}