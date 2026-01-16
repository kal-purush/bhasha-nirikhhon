package ru.abenefic.spring.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import ru.abenefic.spring.model.Product;
import ru.abenefic.spring.services.ProductService;

import java.util.Optional;

@Controller
@RequestMapping("/products")
public class ProductsController {

    private final ProductService productService;

    public ProductsController(ProductService productService) {
        this.productService = productService;
    }

    @GetMapping("/all")
    public String getAllProducts(Model model) {
        model.addAttribute("products", productService.getAll());
        return "all_products";
    }

    @GetMapping("/{id}")
    public String getProductById(@PathVariable String id, Model model) {
        Optional<Product> product = productService.get(Integer.parseInt(id));
        if (product.isPresent()) {
            model.addAttribute("product", product.get());
            return "product";
        } else {
            model.addAttribute("product", id);
            return "product_not_found";
        }
    }

    @GetMapping("/remove/{id}")
    public String deleteProductById(@PathVariable Integer id) {
        productService.delete(id);
        return "redirect:/products/all";
    }

    @PostMapping("/add")
    public String addNewProduct(@ModelAttribute Product product) {
        productService.save(product);
        return "redirect:/products/all";
    }
}