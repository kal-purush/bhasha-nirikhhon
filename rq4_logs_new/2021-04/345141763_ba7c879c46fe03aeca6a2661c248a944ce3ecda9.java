package com.cherniak.controller;

import com.cherniak.persist.Category;
import com.cherniak.persist.CategoryRepository;
import com.cherniak.persist.Product;
import java.io.Serializable;
import java.util.List;
import javax.enterprise.context.SessionScoped;
import javax.inject.Inject;
import javax.inject.Named;

@SessionScoped
@Named
public class CategoryController implements Serializable {

  @Inject
  private CategoryRepository categoryRepository;

  private Category category;

  public Category getCategory() {
    return category;
  }

  public void setCategory(Category category) {
    this.category = category;
  }

  public List<Category> findAll() {
    return categoryRepository.findAll();
  }

  public String editCategory(Category category) {
    this.category = category;
    return "/category_form.xhtml?faces-redirect=true";
  }

  public void deleteCategory(Category category) {
    categoryRepository.delete(category.getId());
  }

  public String saveCategory() {
    categoryRepository.save(category);
    return "/category.xhtml?faces-redirect=true";
  }

  public String addCategory() {
    this.category = new Category();
    return "/category_form.xhtml?faces-redirect=true";
  }
}