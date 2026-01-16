package com.aishwary.springcloud.productService.controllers;

import com.aishwary.springcloud.productService.model.Product;
import com.aishwary.springcloud.productService.repos.ProductRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/productapi")
public class ProductRestController {

    @Autowired
     private ProductRepo repo;

    @RequestMapping(value = "/products", method = RequestMethod.POST)
    public Product create (@RequestBody Product product){
        return repo.save(product);
    }

}