package com.example.pizza.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.beans.factory.annotation.Autowired;

import com.example.pizza.entity.Product;
import com.example.pizza.service.OrderService;
import com.example.pizza.service.ProductService;
import com.example.pizza.service.UserService;

import java.util.List;

@Controller
@RequestMapping("/admin")
public class AdminController {

    @Autowired
    private ProductService productService;

    @Autowired
    private OrderService orderService;

    @Autowired
    private UserService userService;

    // 1. Hiển thị trang Admin Dashboard
    @GetMapping("/dashboard")
    public String dashboard() {
        return "admin/dashboard"; // Trả về file HTML dashboard
    }

    // 2. Quản lý sản phẩm - Hiển thị danh sách sản phẩm
    @GetMapping("/products")
    public String listProducts(Model model) {
        List<Product> products = productService.getAllProducts();
        model.addAttribute("products", products);
        return "admin/products";
    }

    // // 3. Thêm sản phẩm mới - Hiển thị form
    // @GetMapping("/products/new")
    // public String showAddProductForm(Model model) {
    // model.addAttribute("product", new Product());
    // return "admin/add-product";
    // }

    // // 4. Thêm sản phẩm mới - Xử lý dữ liệu
    // @PostMapping("/products")
    // public String saveProduct(@ModelAttribute Product product) {
    // productService.saveProduct(product);
    // return "redirect:/admin/products";
    // }

    // // 5. Sửa sản phẩm - Hiển thị form cập nhật
    // @GetMapping("/products/edit/{id}")
    // public String showEditProductForm(@PathVariable Long id, Model model) {
    // Product product = productService.getProductById(id);
    // model.addAttribute("product", product);
    // return "admin/edit-product";
    // }

    // // 6. Sửa sản phẩm - Xử lý dữ liệu cập nhật
    // @PostMapping("/products/{id}")
    // public String updateProduct(@PathVariable Long id, @ModelAttribute Product
    // product) {
    // productService.updateProduct(id, product);
    // return "redirect:/admin/products";
    // }

    // // 7. Xóa sản phẩm
    // @GetMapping("/products/delete/{id}")
    // public String deleteProduct(@PathVariable Long id) {
    // productService.deleteProduct(id);
    // return "redirect:/admin/products";
    // }

    // // 8. Quản lý đơn hàng - Hiển thị danh sách đơn hàng
    // @GetMapping("/orders")
    // public String listOrders(Model model) {
    // List<Order> orders = orderService.getAllOrders();
    // model.addAttribute("orders", orders);
    // return "admin/orders";
    // }

    // // 9. Quản lý người dùng - Hiển thị danh sách người dùng
    // @GetMapping("/users")
    // public String listUsers(Model model) {
    // List<User> users = userService.getAllUsers();
    // model.addAttribute("users", users);
    // return "admin/users";
    // }
}