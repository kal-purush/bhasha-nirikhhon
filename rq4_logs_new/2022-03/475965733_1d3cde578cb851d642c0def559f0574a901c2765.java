package com.inventory.inventoryService.api;


import com.inventory.inventoryService.exceptions.NotFoundException;
import com.inventory.inventoryService.models.Products;
import com.inventory.inventoryService.service.ProductManager;
import com.inventory.inventoryService.utils.Constant;
import com.inventory.inventoryService.utils.ResponseHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class ProductController {

    @Autowired
    ProductManager productManager;

    @GetMapping("/hello")
    public ResponseEntity<Object> hello() {
        Map<String, String> helloWorld = new HashMap<String, String>();
        helloWorld.put("message", "Hello World");

        return ResponseHandler.generateResponse(helloWorld);
    }

    @GetMapping("/product/{id}")
    public ResponseEntity<Object> getProductById(
            @NotNull(message = "Product Id is Missing") @PathVariable Integer id ) throws NotFoundException {

        Products product = productManager.productById(id);
        return ResponseHandler.generateResponse(product);
    }

    @DeleteMapping("/product/{id}")
    public ResponseEntity<Object> deleteProductById(
            @NotNull(message = "Product Id is Missing") @PathVariable Integer id ) throws NotFoundException {
        String result = productManager.deleteProductById(id);
        return ResponseHandler.generateResponse(result);
    }

    @PostMapping("/product")
    public ResponseEntity<Object> addProduct(@Valid @RequestBody Products products){
        Products product = productManager.addNewProduct(products);
        return ResponseHandler.generateResponse(product);
    }

    @GetMapping("/products")
    public ResponseEntity<Object> getAllProducts(
            @RequestParam(value = "pageNo", defaultValue = Constant.DEFAULT_PAGE_NUMBER, required = false) int pageNo,
            @RequestParam(value = "pageSize", defaultValue = Constant.DEFAULT_PAGE_SIZE, required = false) int pageSize,
            @RequestParam(value = "sortBy", defaultValue = Constant.DEFAULT_PRODUCT_SORT_BY, required = false) String sortBy,
            @RequestParam(value = "sortDir", defaultValue = Constant.DEFAULT_SORT_DIRECTION, required = false) String sortDir

    ) throws NotFoundException {

        return productManager.allProducts(pageNo, pageSize, sortBy, sortDir);

    }

}