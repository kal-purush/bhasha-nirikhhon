package com.toyhe.app.Products.Controllers.Product;

import com.toyhe.app.Products.Dtos.ProductRequest;
import com.toyhe.app.Products.Dtos.ProductResponse;
import com.toyhe.app.Products.Services.ProductService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/av1/products")
public record ProductController(
        ProductService productService
) {

    public ResponseEntity<ProductResponse> createProduct(@RequestBody ProductRequest productRequest) {
        return productService.createProduct(productRequest);
    }

}