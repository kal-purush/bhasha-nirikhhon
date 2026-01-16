package com.integratingProject.financeapp.controllersJSF;

import com.integratingProject.financeapp.models.Category;
import com.integratingProject.financeapp.models.User;
import com.integratingProject.financeapp.repositories.CategoryRepository;
import com.integratingProject.financeapp.services.CategoryService;
import com.integratingProject.financeapp.services.UserService;
import jakarta.annotation.PostConstruct;
import jakarta.faces.view.ViewScoped;
import lombok.Data;
import org.omnifaces.util.Messages;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;

@Component
@ViewScoped
@Data
public class CategoryControllerJSF implements Serializable {
    @Autowired
    private CategoryService categoryService;
    @Autowired
    private UserService userService;
    @Autowired
    private CategoryRepository categoryRepository;

    private Category category;
    private User user;
    private List<Category> categoryList;
    private Integer userId;


    @PostConstruct
    public void init() {
        category = new Category();
    }

    public void insertCategory() {
        User userSelect = userService.findById(userId);
        category.setUser(userSelect);
        categoryService.insert(userSelect.getId(), category);
        Messages.addFlashGlobalInfo("Categoria salva com sucesso");
        category = new Category();
    }

    @PostConstruct
    public List<Category> findAllCategory() {
        return categoryList = categoryRepository.findAll();
    }


}