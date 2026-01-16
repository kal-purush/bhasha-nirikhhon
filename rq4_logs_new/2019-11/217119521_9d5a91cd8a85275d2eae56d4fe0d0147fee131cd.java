package com.es.phoneshop.web;

import com.es.phoneshop.model.product.Product;
import com.es.phoneshop.model.product.ProductService;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class SortProductServlet extends HttpServlet {
    private ProductService productService;

    @Override
    public void init() {
        productService = productService.getInstance();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String field = req.getParameter("order");
        String upOrDown = req.getParameter("sort");
        String query = req.getParameter("query");
        List<Product> findProducts = productService.findProducts(query, field, upOrDown);
        ObjectMapper obj = new ObjectMapper();
        PrintWriter printWriter=resp.getWriter();
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        printWriter.print(obj.writeValueAsString(findProducts));
        printWriter.flush();
    }

    public ProductService getProductService() {
        return productService;
    }

    public void setProductService(ProductService productService) {
        this.productService = productService;
    }
}