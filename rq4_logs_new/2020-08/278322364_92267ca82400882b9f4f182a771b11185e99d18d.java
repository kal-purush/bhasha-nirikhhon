package com.chengfei.book.controller;

import com.chengfei.book.pojo.Cart;
import com.chengfei.book.pojo.User;
import com.chengfei.book.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

@Controller
public class OrderController {
    @Autowired
    OrderService orderService;
    @GetMapping("/createOrder")
    public String createOrder(HttpSession session){
        Cart cart= (Cart) session.getAttribute("cart");
        User user= (User) session.getAttribute("user");
        if(user==null){
            return "forward:/login.html";
        }
        Integer userId=user.getId();
        String orderId= null;

        orderId = orderService.createOrder(cart,userId);

//        request.setAttribute("orderId",orderId);
//            request.getRequestDispatcher("/pages/cart/checkout.jsp").forward(request,response);
        //防止表单重复提交
        session.setAttribute("orderId",orderId);
        return "redirect:/checkout.html";

    }
}