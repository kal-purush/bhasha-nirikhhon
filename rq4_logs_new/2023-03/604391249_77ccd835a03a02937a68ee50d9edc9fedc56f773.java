package com.abneco.delivery.product.controller;

import com.abneco.delivery.product.json.ProductForm;
import com.abneco.delivery.product.service.ProductService;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/product")
@AllArgsConstructor
public class ProductController {

    @Autowired
    private ProductService service;

    @PostMapping("")
    @ResponseStatus(HttpStatus.CREATED)
    public void registerProduct(@RequestBody ProductForm form) {
        service.registerProduct(form);
    }
}