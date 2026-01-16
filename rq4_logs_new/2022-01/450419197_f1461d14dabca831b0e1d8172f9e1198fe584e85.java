package com.example.productService.controller;


import com.example.productService.dto.ProductDto;
import com.example.productService.entity.Category;
import com.example.productService.entity.Product;
import com.example.productService.service.CategoryService;
import com.example.productService.service.ProductService;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping(value = "/product/category")
public class CategoryController {

    @Autowired
    ProductService productService;
    @Autowired
    CategoryService categoryService;

    @GetMapping(value = "/getProductByCategory/{category}")
    List<ProductDto> getProductByCategory(@PathVariable String category) {

        List<ProductDto> productDtos = new ArrayList<>();

        try {
            List<Product> products = productService.getAll();


            for (Product product : products) {
            if(product.getCategory()!=null)
                if (product.getCategory().equals(category)) {

                    ProductDto productDto = new ProductDto();
                    BeanUtils.copyProperties(product, productDto);
                    productDtos.add(productDto);
                }
            }


        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
        return productDtos;

    }

    @GetMapping(value = "/{id}")
    Category getCategoryById(@PathVariable String id){
        return categoryService.get(id);
    }

    @GetMapping()
    List<Category> getAllCategory(){
        return categoryService.getAll();
    }

    @RequestMapping(value = "/add", method={RequestMethod.POST,RequestMethod.PUT})
    public void addUpdateCatefory(@RequestBody Category category)
    {
        categoryService.add(category);
    }



}