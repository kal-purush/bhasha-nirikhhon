package org.example.controller;

import org.example.model.Category;
import org.example.service.categoryService.CategoryService;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@WebServlet(name = "CategoryServlet", urlPatterns = "/categories", loadOnStartup = 1)
public class CategoryServlet extends HttpServlet {
    private CategoryService categoryService;

    @Override
    public void init() {
        categoryService = new CategoryService();
        List<Category> categories = categoryService.getAllCategories();
        getServletContext().setAttribute("categories", categories);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        List<Category> categories = categoryService.getAllCategories();
        request.setAttribute("categories", categories);
        RequestDispatcher dispatcher = request.getRequestDispatcher("navbar.jsp");
        dispatcher.forward(request, response);
    }
}