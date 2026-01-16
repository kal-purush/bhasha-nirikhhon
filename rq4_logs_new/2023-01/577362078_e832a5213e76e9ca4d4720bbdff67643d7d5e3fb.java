package com.geekbrains.controller;

import com.geekbrains.model.ProductPosition;
import com.geekbrains.service.CartService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("api/v1/cart")
public class CartController {

    private CartService cartService;

    @Autowired
    public void setCartService(CartService cartService) {
        this.cartService = cartService;
    }

    @GetMapping
    public List<ProductPosition> getCartContent() {
        return cartService.getList();
    }

    @PostMapping
    public List<ProductPosition> addProductToCart(
            @RequestParam Long id,
            @RequestParam(required = false) Integer amount
    ){
        return cartService.addProductPosition(id, amount);
    }

    @DeleteMapping
    public List<ProductPosition> removeProductFromCart(
            @RequestParam Long id,
            @RequestParam(required = false) Integer amount
    ){
        return cartService.removeProductPosition(id, amount);
    }

}