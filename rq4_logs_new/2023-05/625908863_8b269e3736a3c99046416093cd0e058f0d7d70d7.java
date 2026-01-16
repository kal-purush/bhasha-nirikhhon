package com.brainstrom.meokjang.controller;

import com.brainstrom.meokjang.domain.Food;
import com.brainstrom.meokjang.dto.FoodList;
import com.brainstrom.meokjang.service.FoodService;
import org.springframework.beans.factory.annotation.Autowired;
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
    public FoodList food(@PathVariable Long userId) {
        FoodList foodList = foodService.getList(userId);
        return foodList;
    }

    @PostMapping("/food/add")
    public Food addFood(@RequestBody Food food) {
        return foodService.save(food);
    }

    @DeleteMapping("/food/delete/{foodId}")
    public void deleteFood(@PathVariable Long foodId) {
        foodService.delete(foodId);
    }

    @PutMapping("/food/update/{foodId}")
    public Food updateFood(@PathVariable Long foodId, @RequestBody Food food) {
        return foodService.update(foodId, food);
    }

    @GetMapping("/food/get/{foodId}")
    public Food getFood(@PathVariable Long foodId) {
        return foodService.get(foodId);
    }
}