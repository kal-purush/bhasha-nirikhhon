package com.es.phoneshop.cart;

import com.es.phoneshop.model.product.Product;
import com.es.phoneshop.model.product.ProductNotFoundException;
import com.es.phoneshop.model.product.ProductService;

import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;

public class HttpSessionCartService implements CartService {
    private Cart cart;
    private static HttpSessionCartService cartService;
    private static ProductService productService = ProductService.getInstance();

    private HttpSessionCartService() {
        cart = new Cart();
    }

    public static HttpSessionCartService getInstance() {
        if (cartService == null) {
            cartService = new HttpSessionCartService();
        }
        return cartService;
    }

    @Override
    public Cart getCart(HttpServletRequest request) {
        Cart cart = (Cart) request.getSession().getAttribute("cart");
        if (cart == null) {
            cart = new Cart();
            request.getSession().setAttribute("cart", cart);
        }
        return cart;
    }

    @Override
    public void addCartItem(Cart cart, String idProduct, int quantity) throws ProductNotFoundException {
        Product product = productService.getProduct(idProduct);
        CartItem cartItem = new CartItem(product, quantity);
        if (cart.getListCartItems().contains(cartItem)) {
            int id = cart.getListCartItems().indexOf(cartItem);
            cart.getListCartItems().get(id).setQuantity(cart.getListCartItems().get(id).getQuantity() + quantity);
        } else {
            cart.getListCartItems().add(cartItem);
        }
        cart.setTotalQuantity(cart.getTotalQuantity() + quantity);
        cart.setTotalCost(cart.getTotalCost().add(productService.getProduct(idProduct).getPrice().multiply(new BigDecimal(quantity))));
    }
}

