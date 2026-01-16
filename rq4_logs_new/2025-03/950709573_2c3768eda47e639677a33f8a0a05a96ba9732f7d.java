package me.dio.controller;

import java.net.URI;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import me.dio.domain.model.Produto;
import me.dio.service.ProductService;

@RestController
@RequestMapping("/Produtos")
public class ProductController {

    private final ProductService productService;
    public ProductController(ProductService productService){
        this.productService = productService;
    }

    @GetMapping("/{id}")
    public Produto findById(@PathVariable long id){
        return productService.findById(id);
    }
    
    @PostMapping
    public ResponseEntity<Produto> create(@RequestBody Produto produtoToCreate){
    var productCreated = productService.create(produtoToCreate);
    URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(productCreated.getId())
                .toUri();

                return ResponseEntity.created(location).body(productCreated);
    }



}