package ru.vasili_zlobin.springdatajpa.conrollers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ru.vasili_zlobin.springdatajpa.model.Product;
import ru.vasili_zlobin.springdatajpa.services.ProductService;

import java.util.List;

@RestController
@RequestMapping("/products")
public class ProductController {
    @Autowired
    private ProductService service;

    @GetMapping
    public List<Product> getListProducts(@RequestParam(defaultValue = "0") Double min, @RequestParam(defaultValue = "0") Double max) {
        return service.getProducts(min, max);
    }

    @GetMapping("/{id}")
    public Product getProduct(@PathVariable Long id) {
        return service.getProductById(id);
    }

    @PostMapping
    public void addProduct(@RequestBody Product product) {
        service.addProduct(product);
    }

    @GetMapping("/delete/{id}")
    public void deleteById(@PathVariable Long id) {
        service.deleteById(id);
    }
}