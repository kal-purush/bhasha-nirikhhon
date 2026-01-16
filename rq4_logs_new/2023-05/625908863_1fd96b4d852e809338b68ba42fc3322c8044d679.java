package com.brainstrom.meokjang.common.controller;

import com.brainstrom.meokjang.common.dto.request.RecipeRequest;
import com.brainstrom.meokjang.common.dto.response.ApiResponse;
import com.brainstrom.meokjang.common.service.RecipeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RecipeController {

    private final RecipeService recipeService;

    @Autowired
    public RecipeController(RecipeService recipeService) {
        this.recipeService = recipeService;
    }

    @PostMapping("/recipe")
    public ResponseEntity<ApiResponse> getRecipe(@RequestBody RecipeRequest foodList, BindingResult result) {

        if (result.hasErrors())
            return ResponseEntity.badRequest().body(new ApiResponse(400, "요청 형식이 잘못되었습니다.", null));
        try {
            ApiResponse apiResponse = new ApiResponse(200, "레시피 조회 성공", recipeService.getRecipe(foodList));
            return ResponseEntity.ok(apiResponse);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "레시피 조회 실패", null));
        }
    }
}