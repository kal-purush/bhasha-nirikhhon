package com.koscom.demo.controller;

import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.koscom.demo.protocol.domain.User;
import com.koscom.demo.protocol.response.GetUserResponseDto;
import com.koscom.demo.service.TestService;

import lombok.RequiredArgsConstructor;

@Controller
@RequiredArgsConstructor
@RequestMapping("/index")
public class TestController {
    
    private final TestService testService;

    @GetMapping
    public String testController(Model model) {
        List<GetUserResponseDto> items = testService.getUser(); 
        model.addAttribute("items", items);
        return "basic/test";
    }

    @GetMapping("/move/add")
    public String redirectAddController(Model model) {
        return "basic/add";
    }

    // ajax로 보낼 시 => @Requestbody 사용 O
    // form의 submit으로 보낼 시 => @Requestbody 사용 X
    @PostMapping("/add")
    public String addUserController(User user, Model model) {
        testService.addUser(user);
        
        List<GetUserResponseDto> items = testService.getUser(); 
        model.addAttribute("items", items);

        return "redirect:/index/";
    }

    @GetMapping("/delete/{userId}")
    public String deleteUserController(@PathVariable String userId) {
        testService.deleteUser(userId);
        return "redirect:/index/";
    }

    @GetMapping("/move/update/{userId}")
    public String redirectUpdateController(@PathVariable String userId, Model model) {
        GetUserResponseDto item = testService.getOneUser(userId); 
        model.addAttribute("item", item);
        return "basic/update";
    }

    @PostMapping("/update")
    public String updateUserController(User user) {
        testService.updateUser(user);

        return "redirect:/index/";
    }
}