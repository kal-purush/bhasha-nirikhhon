package ru.romankuznetsov.controller;

import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.annotation.SessionScope;
import ru.romankuznetsov.dto.ProductDto;
import ru.romankuznetsov.entity.Product;
import ru.romankuznetsov.repository.ProductRepository;

import java.util.List;
import java.util.NoSuchElementException;

@RestController
@SessionScope
@RequestMapping("api/v1/product")
public class ProductController {

    private final ProductRepository productRepository;

    public ProductController(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @GetMapping
    public List<Product> getProducts(){
        return productRepository.findAll();
    }

    @GetMapping("{id}")
    public Product findProduct(@PathVariable long id){
        return productRepository.findById(id).orElseThrow(NoSuchElementException::new);
    }

    @PostMapping
    public Product saveProduct(@RequestBody ProductDto productDto){
        Product product = new Product();
        product.setTitle(productDto.getTitle());
        product.setPrice(productDto.getPrice());
        if (productDto.getTitle() != null && productDto.getPrice() != null) {
            productRepository.save(product);
        }
        return product;
    }

    @PutMapping
    public Product updateProduct(@RequestBody Product product){
        productRepository.save(product);
        return product;
    }

    @DeleteMapping("{id}")
    public void deletePerson(@PathVariable long id){
        productRepository.delete(productRepository.findById(id).orElseThrow(NoSuchElementException::new));
    }
}