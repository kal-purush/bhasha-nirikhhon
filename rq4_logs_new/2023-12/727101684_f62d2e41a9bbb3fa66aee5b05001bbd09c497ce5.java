package com.example.demo.recipeapi.api;

import com.example.demo.recipeapi.service.RecipeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/api/recipe")
@CrossOrigin
public class RecipeController {

    private final RecipeService recipeService;


    // 레시피 리스트 요청 처리
    @GetMapping
    public ResponseEntity<?> getRecipeList(){
        log.info("/api/recipe GET list request");
        recipeService.getRecipeList();

        return null;
    }

    // 레시피 상세보기 요청 처리





}