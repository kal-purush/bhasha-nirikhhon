package com.example.bibimbapcloud.controller;

import com.example.bibimbapcloud.domain.Bibimbap;
import com.example.bibimbapcloud.domain.Ingredient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.validation.Valid;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Controller
@RequestMapping("/design")
public class DesignBibimbapController {

    @GetMapping
    public String showDesignForm(Model model) {
        List<Ingredient> ingredients = Arrays.asList(
          new Ingredient("KORC", "Korean Rice", Ingredient.Type.RICE),
          new Ingredient("VIRC", "Vietnam Rice", Ingredient.Type.RICE),
          new Ingredient("GRBF", "Ground Beef", Ingredient.Type.PROTEIN),
          new Ingredient("GHPK", "Ground Pork", Ingredient.Type.PROTEIN),
          new Ingredient("CART", "Carrot", Ingredient.Type.VEGGIES),
          new Ingredient("SPNC", "spinach", Ingredient.Type.VEGGIES),
          new Ingredient("CHED", "Cheddar", Ingredient.Type.CHEESE),
          new Ingredient("JACK", "Monterrey Jack", Ingredient.Type.CHEESE),
          new Ingredient("KOCJ", "Kochujang", Ingredient.Type.SAUCE),
          new Ingredient("MISO", "Miso", Ingredient.Type.SAUCE)
        );

        Ingredient.Type[] types = Ingredient.Type.values();
        for(Ingredient.Type type : types) {
            model.addAttribute(type.toString().toLowerCase(), filterByType(ingredients, type));
        }

        model.addAttribute("bibimbap", new Bibimbap());

        return "design";
    }

    private List<Ingredient> filterByType(List<Ingredient> ingredients, Ingredient.Type type) {

        return ingredients
                .stream()
                .filter(x -> x.getType().equals(type))
                .collect(Collectors.toList());
    }

    @PostMapping
    public String processDesign(@Valid Bibimbap bibimbap, Errors errors) {
        if(errors.hasErrors()) {
            return "design";
        }

        log.info("Processing design: " + bibimbap);
        return "redirect:/orders/current";
    }
}