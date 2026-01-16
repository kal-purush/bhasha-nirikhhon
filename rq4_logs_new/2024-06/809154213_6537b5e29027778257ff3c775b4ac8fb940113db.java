package de.ait_tr.g_40_shop.controller;

import de.ait_tr.g_40_shop.domain.entity.Product;
import de.ait_tr.g_40_shop.service.interfaces.ProductService;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/products")
public class ProductController {

    private ProductService service;

    public ProductController(ProductService service) {
        this.service = service;
    }

    @PostMapping("/example")
    public Product save(@RequestBody Product product) {
        return service.save(product);
    }
    @GetMapping
    public List<Product> get(@RequestParam(required = false) Long id) {
        if (id==null) {
            return service.getAllActiveProducts();
        } else {
            return List.of(service.getById(id));
        }
    }
    @PutMapping
    public Product update(@RequestBody Product product) {
        return service.update(product);
    }

    @DeleteMapping
    public void delete(
            @RequestParam(required = false) Long id,
            @RequestParam(required = false) String title
            ) {
        if (id != null) {
            service.deleteById(id);
        } else if (title != null) {
            service.deleteByTitle(title);
        }
    }
    @PutMapping("/restore")
    public void restore(@RequestParam Long id) {
        service.restoreById(id);
    }
    @GetMapping("/quantity")
    public long getQuantity() {
        return service.getActiveProductsQuantity();
    }
    @GetMapping("/total-Price")
    public BigDecimal getTotalPrice() {
        return service.getActiveProductsTotalPrice();
    }

    @GetMapping("/average-Price")
    public BigDecimal getAveragePrice() {
        return service.getActiveProductsTotalPrice();
    }
}