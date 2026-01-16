package com.brainstrom.meokjang.food.controller;

import com.brainstrom.meokjang.common.dto.response.ApiResponse;
import com.brainstrom.meokjang.food.dto.request.FoodRequest;
import com.brainstrom.meokjang.dto.OcrList;
import com.brainstrom.meokjang.food.dto.request.OcrRequest;
import com.brainstrom.meokjang.food.dto.response.FoodResponse;
import com.brainstrom.meokjang.food.dto.response.OcrResponse;
import com.brainstrom.meokjang.food.service.FoodService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class FoodController {

    private FoodService foodService;

    @Autowired
    public FoodController(FoodService foodService) {
        this.foodService = foodService;
    }

    @ResponseBody
    @GetMapping("/food/{userId}")
    public ResponseEntity<ApiResponse> getFoodList(@PathVariable Long userId) {
        List<FoodResponse> foodList = foodService.getList(userId);
        ApiResponse apiResponse = new ApiResponse(200, "냉장고 조회 성공", foodList);
        return ResponseEntity.ok(apiResponse);
    }

    @PostMapping("/food/add")
    public ResponseEntity<ApiResponse> addFood(@RequestBody FoodRequest foodRequest) {
        FoodResponse food = foodService.save(foodRequest);
        ApiResponse apiResponse = new ApiResponse(200, "음식 추가 성공", food);
        return ResponseEntity.ok(apiResponse);
    }

    @PostMapping("/food/addList")
    public ResponseEntity<ApiResponse> addFoodList(@RequestBody List<FoodRequest> foodList) {
        List<FoodResponse> savedList = foodService.saveList(foodList);
        ApiResponse apiResponse = new ApiResponse(200, "음식 추가 성공", savedList);
        return ResponseEntity.ok(apiResponse);
    }

    @DeleteMapping("/food/delete/{foodId}")
    public ResponseEntity<ApiResponse> deleteFood(@PathVariable Long foodId) {
        foodService.delete(foodId);
        ApiResponse apiResponse = new ApiResponse(200, "음식 삭제 성공", null);
        return ResponseEntity.ok(apiResponse);
    }

    @PutMapping("/food/update/{foodId}")
    public ResponseEntity<ApiResponse> updateFood(@PathVariable Long foodId, @RequestBody FoodRequest foodRequest) {
        FoodResponse updatedFood = foodService.update(foodId, foodRequest);
        ApiResponse apiResponse = new ApiResponse(200, "음식 수정 성공", updatedFood);
        return ResponseEntity.ok(apiResponse);
    }

    @GetMapping("/food/get/{foodId}")
    public ResponseEntity<ApiResponse> getFood(@PathVariable Long foodId) {
        FoodResponse food = foodService.get(foodId);
        ApiResponse apiResponse = new ApiResponse(200, "음식 조회 성공", food);
        return ResponseEntity.ok(apiResponse);
    }

    @ResponseBody
    @PostMapping("/food/recommend")
    public ResponseEntity<ApiResponse> recommendFood(@RequestBody OcrRequest ocrRequest) {
        OcrList ocrList = foodService.extractFood(ocrRequest);
        List<OcrResponse> result = foodService.recommend((ocrList));
        ApiResponse apiResponse = new ApiResponse(200, "스마트 등록 성공", result);
        return ResponseEntity.ok(apiResponse);
    }
}