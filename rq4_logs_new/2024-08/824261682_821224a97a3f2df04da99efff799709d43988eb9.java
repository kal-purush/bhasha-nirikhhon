package br.com.soat8.techchallenge.adapter.in.controller;

import br.com.soat8.techchallenge.core.port.in.ProductCategoryUseCase;

import br.com.soat8.techchallenge.domain.ProductCategory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/lanchonete")
public class ProductCategoryController {

    private final ProductCategoryUseCase productCategoryUseCase;

    public ProductCategoryController(ProductCategoryUseCase productCategoryUseCase) {
        this.productCategoryUseCase = productCategoryUseCase;
    }

    @GetMapping("/categoria/{categoriaId}")
    public ResponseEntity<ProductCategory> cadastrarCliente(@PathVariable("categoriaId") UUID categoriaId) {
        ProductCategory productCategory = productCategoryUseCase.findProductCategory(categoriaId);
        return ResponseEntity.ok(productCategory);
    }

}