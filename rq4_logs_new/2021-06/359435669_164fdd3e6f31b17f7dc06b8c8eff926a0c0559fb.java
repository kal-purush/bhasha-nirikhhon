package com.coffee.coffee_data_aggregator.controllers;

import com.coffee.coffee_data_aggregator.repository.UserRepository;
import com.coffee.coffee_data_aggregator.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController
public class OrderRestController {
    @Autowired
    private OrderService orderService;
    @Autowired
    private UserRepository userRepository;

    @PostMapping("/api/order/add/{pid}/{qu}")
    public @ResponseBody String addProductOrder(@PathVariable("pid") Integer productID,
                                                @PathVariable("qu") Integer quantity,
                                                @AuthenticationPrincipal Authentication aut){
        // TODO add user!!
//        User user
//        orderService.addProduct(productID,quantity,user);

        return "Product Added";
    }

    @PostMapping("/api/order/update/{pid}/{qu}")
    public @ResponseBody String updateQuantity(@PathVariable("pid") Integer productID,
                                                @PathVariable("qu") Integer quantity,
                                                @AuthenticationPrincipal Authentication aut){
        //user add
//        orderService.updateQuantity(productID,quantity,user);


        return "product Update";
    }

    @DeleteMapping ("/api/order/delete/{pid}")
    public @ResponseBody String removeProductFromOrder(@PathVariable("pid") Integer productID){
        // TODO add user!!
//        User user
//        orderService.removeProduct(productID, user);
        return "productRemove";
    }

}