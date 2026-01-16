package com.example.cssebackend.Controller;

import com.example.cssebackend.Model.Cart;
import com.example.cssebackend.Model.Order;
import com.example.cssebackend.Service.CartService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(origins = "http://localhost:3000")
@RequestMapping("/cart")
public class CartController {

    @Autowired
    private final CartService cartService;

    public CartController(CartService cartService) {
        this.cartService = cartService;
    }

    @PostMapping("/")
    public ResponseEntity<?> addToCart(@RequestBody Cart cart){
        try {
            cartService.addToCart(cart);
            return new ResponseEntity<>(HttpStatus.OK);
        }
        catch (Exception e){
            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/{vendorId}")
    public ResponseEntity<Object> getCartById(@PathVariable String vendorId){
        try {
            return new ResponseEntity<>(cartService.getCartById(vendorId), HttpStatus.OK);
        }
        catch (Exception e){
            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @PutMapping({"/{vendorId}/{itemId}"})
    public ResponseEntity<Object> removeItemFromCart(@PathVariable String vendorId, @PathVariable String itemId){
        try {
            cartService.removeItemFromCarta(vendorId,itemId);
            return new ResponseEntity<>(HttpStatus.OK);
        }
        catch (Exception e){
            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }
}