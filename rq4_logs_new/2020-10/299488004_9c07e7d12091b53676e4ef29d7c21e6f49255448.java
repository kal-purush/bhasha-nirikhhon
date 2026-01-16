package com.cartas.jaktani.controller;

import com.cartas.jaktani.dto.CategoryDto;
import com.cartas.jaktani.exceptions.ResourceNotFoundException;
import com.cartas.jaktani.service.CategoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "/api/category")
public class CategoryController {
    @Autowired
    CategoryService categoryService;

    @GetMapping(path = "/id/{id}")
    public Object getCategoryByID(@PathVariable(name = "id") Integer id) throws ResourceNotFoundException {
        return categoryService.getCategoryByID(id);
    }

    @GetMapping(path = "/authentication/all")
    public Object getAllCategory() {
        return categoryService.getAllCategorys();
    }

	
	@PostMapping(path = "/add") 
	public Object addCategory(@RequestBody CategoryDto category) { 
		return categoryService.addCategory(category); 
	}
	 

	@PostMapping(path = "/edit/{id}") 
	public Object editCategory(@RequestBody CategoryDto category) { 
		return categoryService.editCategory(category); 
	}

    @PostMapping(path = "/delete/{id}")
    public Object deleteCategoryByID(@PathVariable(name = "id") Integer id) throws ResourceNotFoundException {
        return categoryService.deleteCategoryByID(id);
    }
    
    @GetMapping(path = "/authentication/allWithSubCategoryByID/{id}")
    public Object getAllWithSubCategoryByID(@PathVariable(name = "id") Integer id) throws ResourceNotFoundException {
        return categoryService.getAllWithSubCategoryById(id);
    }

}