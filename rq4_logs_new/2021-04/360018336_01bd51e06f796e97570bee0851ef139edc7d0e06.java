package ru.geekbrains.mvc.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import ru.geekbrains.mvc.dto.Product;
import ru.geekbrains.mvc.service.ProductService;

@Controller
@RequestMapping("/product")
@RequiredArgsConstructor
@Log4j2
public class CartController {
    private final ProductService productService;

    @PostMapping("/add")
    public String addProduct(@RequestParam int id, String title, int cost) {
        productService.add(id, title, cost);
        return "cart";


    }
}