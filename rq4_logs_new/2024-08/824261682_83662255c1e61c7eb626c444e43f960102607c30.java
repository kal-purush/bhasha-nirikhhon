package br.com.soat8.techchallenge.adapter.in.controller;

import br.com.soat8.techchallenge.core.port.in.ProductUseCase;
import br.com.soat8.techchallenge.domain.Product;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(ProductController.BASE_URL)
public class ProductController {

    public static final String BASE_URL = "/lanchonete/product";

    private final ProductUseCase productUseCase;

    public ProductController(ProductUseCase productUseCase) {
        this.productUseCase = productUseCase;
    }

    @PostMapping
    public ResponseEntity<Void> createProduct(@Valid @RequestBody Product product) {

        productUseCase.saveProduct(product);
        return new ResponseEntity<>(HttpStatus.CREATED);

    }

    @PutMapping
    public ResponseEntity<Product> updateProduct(@Valid @RequestBody Product product) {

        productUseCase.updateProduct(product);
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @DeleteMapping
    public ResponseEntity<Product> removeProduct(@Valid @RequestBody Integer product_id) {

        productUseCase.removeProduct(product_id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

}