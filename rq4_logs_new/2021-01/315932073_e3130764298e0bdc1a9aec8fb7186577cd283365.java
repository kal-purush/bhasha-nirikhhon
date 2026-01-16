package com.threejavers.drinkapp.web.controller;

import com.threejavers.drinkapp.web.dto.CategoryDto;
import com.threejavers.drinkapp.web.dto.DrinkDto;
import com.threejavers.drinkapp.web.dto.IngredientDto;
import com.threejavers.drinkapp.file.service.FileDataHandlerService;
import com.threejavers.drinkapp.service.CategoryService;
import com.threejavers.drinkapp.service.DrinkService;
import com.threejavers.drinkapp.service.IngredientService;
import com.threejavers.drinkapp.service.MessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.Part;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequiredArgsConstructor
@Slf4j
public class AdminController {

    public static final String ADMIN_PANEL = "admin-panel";
    public static final String MESSAGE = "message";
    private final DrinkService drinkService;
    private final MessageService messageService;
    private final CategoryService categoryService;
    private final IngredientService ingredientService;
    private final FileDataHandlerService fileDataHandlerService;

    @GetMapping("/admin-panel")
    public String showAdminPanel(Model model) {
        addAttributes(model);
        model.addAttribute(MESSAGE, messageService.get(3L));
        return ADMIN_PANEL;
    }

    @PostMapping("json-upload")
    public String jsonUpload(@RequestParam(name = "drinks") Part jsonFile, Model model) {
        try {
            fileDataHandlerService.dataUploadHandler(jsonFile);
        } catch (IOException e) {
            log.error("The file wasn't loaded");
        }
        addAttributes(model);
        model.addAttribute(MESSAGE, messageService.get(1L));
        return ADMIN_PANEL;
    }

    @PostMapping("/save-drink")
    public String saveDrink(
            @RequestParam(name = "name") String name,
            @RequestParam(name = "recipe") String recipe,
            @RequestParam(name = "drinkType") String drinkType,
            @RequestParam(name = "glassType") String glassType,
            @RequestParam(name = "imageUrl") String imageUrl,
            @RequestParam(name = "category") String category,
            @RequestParam(name = "ingredient") String[] ingredient,
            @RequestParam(name = "measure") String[] measure,
            Model model
    ) {
        drinkService.save(newDrinkDto(name, recipe, drinkType, glassType, imageUrl, category, newIngredientDtoList(ingredient, measure)));
        addAttributes(model);
        model.addAttribute(MESSAGE, messageService.get(1L));
        return ADMIN_PANEL;
    }

    @PostMapping("/delete-drink")
    public String deleteDrink(
            @RequestParam(name = "name") String name,
            Model model
    ) {
        drinkService.delete(name);
        addAttributes(model);
        model.addAttribute(MESSAGE, messageService.get(1L));
        return ADMIN_PANEL;
    }

    private void addAttributes(Model model) {
        model.addAttribute("drinks", drinkService.getAllDrinks());
        model.addAttribute("categories", categoryService.getUniqueCategoryNames());
        model.addAttribute("glasses", drinkService.getUniqueGlass());
        model.addAttribute("ingredients", ingredientService.getUniqueIngredientNames());
    }

    private List<IngredientDto> newIngredientDtoList(String[] ingredient, String[] measure) {
        List<IngredientDto> ingredientDtoList = new ArrayList<>();
        for (int ing = 0; ing < ingredient.length; ing++) {
            ingredientDtoList.add(IngredientDto.builder()
                    .name(ingredient[ing])
                    .measure(measure[ing])
                    .build());
        }
        return ingredientDtoList;
    }

    private DrinkDto newDrinkDto(String name, String recipe, String drinkType, String glassType, String imageUrl,
                                 String category, List<IngredientDto> ingredientList) {
        return DrinkDto.builder()
                .name(name)
                .isCustom(true)
                .isApproved(true)
                .recipe(recipe)
                .drinkType(drinkType)
                .glassType(glassType)
                .imageUrl(imageUrl)
                .category(CategoryDto.builder().name(category).build())
                .ingredientList(ingredientList)
                .build();
    }
}