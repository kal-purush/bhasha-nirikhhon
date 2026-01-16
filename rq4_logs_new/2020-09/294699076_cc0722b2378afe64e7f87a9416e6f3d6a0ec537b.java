package com.boiechko.controller;

import com.boiechko.model.Product;
import com.boiechko.service.interfaces.ClothesFilterService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpSession;
import java.util.Arrays;
import java.util.List;

@Controller
@RequestMapping(value = "/clothesFilter")
public class ClothesFiltersController {

    private final ClothesFilterService clothesFilterService;

    public ClothesFiltersController(ClothesFilterService clothesFilterService) {
        this.clothesFilterService = clothesFilterService;
    }

    @GetMapping()
    public String onClothesFilters(final Model model) {

        final ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        final HttpSession session = attributes.getRequest().getSession();

        final List<Product> clothes = (List<Product>) session.getAttribute("clothes");

        model.addAttribute("brands", clothesFilterService.getAllBrandsOfClothes(clothes));
        model.addAttribute("colors", clothesFilterService.getAllColorsOfClothes(clothes));
        model.addAttribute("sizes", Arrays.asList("XS", "S", "M", "L", "XL", "XXL"));

        return "components/clothesFilters";
    }

}