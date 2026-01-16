package com.cartas.jaktani.controller;

import com.cartas.jaktani.dto.ProductDto;
import com.cartas.jaktani.exceptions.ResourceNotFoundException;
import com.cartas.jaktani.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "/api/product")
public class ProductController {
    @Autowired
    ProductService productService;

    @GetMapping(path = "/id/{id}")
    public Object getProductByID(@PathVariable(name = "id") Integer id) throws ResourceNotFoundException {
        return productService.getProductByID(id);
    }

    @GetMapping(path = "/all")
    public Object getAllProduct() {
        return productService.getAllProducts();
    }

	
	@PostMapping(path = "/add") 
	public Object addProduct(@RequestBody ProductDto product) { 
		return productService.addProduct(product); 
	}
	 

	@PostMapping(path = "/edit/{id}") 
	public Object editProduct(@RequestBody ProductDto product) { 
		return productService.editProduct(product); 
	}

    @PostMapping(path = "/delete/{id}")
    public Object deleteProductByID(@PathVariable(name = "id") Integer id) throws ResourceNotFoundException {
        return productService.deleteProductByID(id);
    }

}