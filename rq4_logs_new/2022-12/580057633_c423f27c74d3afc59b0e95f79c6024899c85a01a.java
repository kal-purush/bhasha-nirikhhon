package com.vitweb.vitwebapi.adapter.web.v1.controller;

import com.vitweb.vitwebapi.adapter.web.base.RestApiV1;
import com.vitweb.vitwebapi.adapter.web.base.VsResponseUtil;
import com.vitweb.vitwebapi.application.constants.UrlConstant;
import com.vitweb.vitwebapi.application.inputs.category.CreateCategoryInput;
import com.vitweb.vitwebapi.application.inputs.category.UpdateCategoryInput;
import com.vitweb.vitwebapi.application.services.ICategoryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestApiV1
public class CategoryController {

  private final ICategoryService categoryService;

  public CategoryController(ICategoryService categoryService) {
    this.categoryService = categoryService;
  }

  @GetMapping(UrlConstant.Category.LIST)
  public ResponseEntity<?> getAll() {
    return VsResponseUtil.ok(categoryService.getAll());
  }

  @GetMapping(UrlConstant.Category.GET)
  public ResponseEntity<?> getCategoryById(@PathVariable("id") Long id) {
    return VsResponseUtil.ok(categoryService.getCategoryById(id));
  }

  @PostMapping(UrlConstant.Category.CREATE)
  public ResponseEntity<?> createCategory(@ModelAttribute CreateCategoryInput createCategoryInput) {
    return VsResponseUtil.ok(categoryService.createCategory(createCategoryInput));
  }

  @PatchMapping(UrlConstant.Category.UPDATE)
  public ResponseEntity<?> updateCategory(@Valid @ModelAttribute UpdateCategoryInput updateCategoryInput) {
    return VsResponseUtil.ok(categoryService.updateCategory(updateCategoryInput));
  }

  @DeleteMapping(UrlConstant.Category.DELETE)
  public ResponseEntity<?> deleteCategory(@PathVariable("id") Long id) {
    return VsResponseUtil.ok(categoryService.deleteById(id));
  }
}