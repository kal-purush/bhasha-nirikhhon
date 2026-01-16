package com.BackendChallenge.TechTrendEmporium.controller;

import com.BackendChallenge.TechTrendEmporium.controller.Requests.WishlistRequest;
import com.BackendChallenge.TechTrendEmporium.entity.Product;
import com.BackendChallenge.TechTrendEmporium.entity.Wishlist;
import com.BackendChallenge.TechTrendEmporium.entity.WishlistProduct;
import com.BackendChallenge.TechTrendEmporium.service.Response.WishlistResponse;
import com.BackendChallenge.TechTrendEmporium.service.WishlistService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/user")
public class WishlistController {

    @Autowired
    private WishlistService wishlistService;

    @GetMapping(value = "wishlist")
    public ResponseEntity<?> getWishlist(@RequestBody WishlistRequest request) {
        WishlistResponse wishlistProducts = wishlistService.getWishlistByUser(request.getUser_id());
        if (wishlistProducts != null) {
            return new ResponseEntity<>(wishlistProducts, HttpStatus.OK);
        } else {
            return ResponseEntity.ok("Wishlist empty");
        }
    }


    @PostMapping(value = "wishlist/add/{product_id}")
    public ResponseEntity<String> addProductToWishlist(@RequestBody WishlistRequest request, @PathVariable("product_id") Long productId){
        boolean added = wishlistService.addProductToWishlist(request.getUser_id(), productId);
        if (added) {
            return ResponseEntity.ok("Product added to wishlist!");
        } else {
            return ResponseEntity.ok("ERROR : Product not added to wishlist.");
        }
    }

    @DeleteMapping(value = "wishlist/remove/{product_id}")
    public ResponseEntity<String> removeProductFromWishlist(@RequestBody WishlistRequest request, @PathVariable("product_id") Long productId){
        boolean removed = wishlistService.removeProductFromWishlist(request.getUser_id(), productId);
        if (removed) {
            return ResponseEntity.ok("Product removed from wishlist");
        } else {
            return ResponseEntity.ok("User or product not found. Product not removed from wishlist.");
        }
    }

}