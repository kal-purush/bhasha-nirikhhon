package com.BackendChallenge.TechTrendEmporium.controller;

import com.BackendChallenge.TechTrendEmporium.controller.Requests.CartRequest;
import com.BackendChallenge.TechTrendEmporium.service.CartService;
import com.BackendChallenge.TechTrendEmporium.service.Response.CartResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/cart")
public class CartController {

    @Autowired
    private CartService cartService;

    @GetMapping(value = "all")
    public ResponseEntity<?> getCarts(@RequestBody CartRequest request) {
        return ResponseEntity.ok(cartService.getCartByUser(request.getUser_id()));
    }

    @PostMapping(value = "add")
    public ResponseEntity<?> addProductToCart(@RequestBody CartRequest request){
        boolean added = cartService.addProductToCart(request.getUser_id(), request.getProduct_id());
        if (added) {
            return ResponseEntity.created(null).body("Product added to cart!");
        } else {
            return ResponseEntity.badRequest().body("ERROR : Product not added to cart.");
        }
    }

    @DeleteMapping(value = "remove")
    public ResponseEntity<?> removeProductFromCart(@RequestBody CartRequest request){
        boolean removed = cartService.deleteProductFromCart(request.getUser_id(), request.getProduct_id());
        if (removed) {
            return ResponseEntity.ok("Product removed from cart");
        } else {
            return ResponseEntity.badRequest().body("User or product not found. Product not removed from cart.");
        }
    }
}