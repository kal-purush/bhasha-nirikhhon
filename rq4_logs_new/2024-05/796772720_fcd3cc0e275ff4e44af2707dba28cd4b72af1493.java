package com.example.product.orders.controller;

import com.example.product.orders.model.dto.ProductRequestDto;
import com.example.product.orders.model.service.ProductService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/products")
public class ProductController {
    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping("/add_product")
    public ProductRequestDto addProduct(@RequestBody ProductRequestDto productRequestDto) {
        productService.addProduct(productRequestDto);
        return productRequestDto;
    }

//    @GetMapping
//    public List<ProductResponseDto> findAllProduct(){
//        return productService.findALLpProducts().stream()
//                .map(productEntity -> productMapper.fromEntity(productEntity))
//                .toList();
//    }
}