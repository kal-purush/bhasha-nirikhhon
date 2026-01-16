package com.example.storeeverything.Admin;

import com.example.storeeverything.Category.Category;
import com.example.storeeverything.Category.CategoryRepository;
import com.example.storeeverything.Information.InformationRepository;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Comparator;
import java.util.List;


@Controller
@RequestMapping("/admin")

public class AdminController {

    @GetMapping("")
    public String adminPanel(){
        return "admin/admin";
    }




}