package com.kodilla.ecommerce.controller;

import com.kodilla.ecommerce.dto.CartDto;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping(value = "/v1/carts", consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
public class CartController {

    @GetMapping
    public List<CartDto> getCarts() {
        return Arrays.asList(
                CartDto.builder()
                .id(1L)
                .productId(1L)
                .userId(1L)
                .quantity(10)
                .build()
        );
    }

    @GetMapping("/{id}")
    public CartDto getCart(@PathVariable Long id) {
        return CartDto.builder()
                .id(1L)
                .build();
    }

    @PostMapping
    public CartDto createCart(@RequestBody final CartDto cartDto) {
        return CartDto.builder()
                .id(1L)
                .productId(1L)
                .userId(1L)
                .quantity(10)
                .build();
    }

    @PutMapping
    public CartDto addProductToCart(@RequestBody final CartDto cartDto) {
        return CartDto.builder()
                .productId(1L)
                .build();
    }

    @DeleteMapping("/{productId}/delete")
    public void deleteProductFromCart(@PathVariable Long productId){
    }

    @DeleteMapping("/{id}")
    public void deleteCart(@PathVariable final Long id) {
    }




}
