package com.example.backendeindopdracht.controller;


import com.example.backendeindopdracht.DTO.inputDto.ProductInputDTO;
import com.example.backendeindopdracht.DTO.outputDto.ProductOutputDTO;
import com.example.backendeindopdracht.service.ProductService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }
    @PostMapping("/add")
    public ResponseEntity<ProductOutputDTO> addProduct (@RequestBody ProductInputDTO productInputDto){
        ProductOutputDTO addedProduct = productService.addProduct(productInputDto);
        return ResponseEntity.status(HttpStatus.CREATED).body(addedProduct);
    }


}