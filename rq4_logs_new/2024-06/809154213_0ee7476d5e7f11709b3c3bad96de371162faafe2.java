package de.ait_tr.g_40_shop.controller;

import de.ait_tr.g_40_shop.domain.dto.ProductSupplyDto;
import de.ait_tr.g_40_shop.service.interfaces.ProductService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/system")
public class SystemController {

    private final ProductService productService;

    public SystemController(ProductService productService) {
        this.productService = productService;
    }

    @GetMapping("/products")
    public List<ProductSupplyDto> getAllProducts() {
       return productService.getProductsForSupply();
    }
}