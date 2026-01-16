package com.example.controller;

import com.example.model.Food;
import com.example.repository.FoodRepository;
import com.example.service.FoodService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/food")
public class FoodController {
    private final FoodService foodService;
    private final FoodRepository foodRepository;

    public FoodController(FoodService foodService, FoodRepository foodRepository) {
        this.foodService = foodService;
        this.foodRepository = foodRepository;
    }

    @GetMapping("/{id}")
    public ResponseEntity<Food> getFoodById(@PathVariable Integer id) {
        Food food = foodService.getFoodById(id);
        if (food != null) {
            return ResponseEntity.ok(food);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/cook")
    public ResponseEntity<Food> cookFood(@RequestBody Food food) {
        Food cookedFood = foodRepository.save(food);
        return ResponseEntity.ok(cookedFood);
    }

    @PutMapping("/change-compound/{id}")
    public ResponseEntity<Food> changeCompound(@PathVariable int id, @RequestBody Food food) {
        Optional<Food> optionalFood = foodRepository.findById((long) id);
        if (optionalFood.isPresent()) {
            Food existingFood = optionalFood.get();
            existingFood.setName(food.getName());
            existingFood.setType(food.getType());
            existingFood.setCompound(food.getCompound());
            existingFood.setCalories(food.getCalories());
            existingFood.setPrice(food.getPrice());
            existingFood.setDescription(food.getDescription());
            existingFood.setVegetarian(food.isVegetarian());
            Food updatedFood = foodRepository.save(existingFood);
            return ResponseEntity.ok(updatedFood);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/delete-from-menu/{id}")
    public ResponseEntity<Void> deleteFoodFromMenu(@PathVariable int id) {
        foodRepository.deleteById((long) id);
        return ResponseEntity.noContent().build();
    }

    @PatchMapping("/{id}")
    public ResponseEntity<Food> updateFoodPartially(@PathVariable int id, @RequestBody Food updatedFood) {
        Optional<Food> optionalFood = foodRepository.findById((long) id);
        if (!optionalFood.isPresent()) {
            return ResponseEntity.notFound().build();
        }

        Food existingFood = optionalFood.get();
        if (updatedFood.getName() != null) {
            existingFood.setName(updatedFood.getName());
        }
        if (updatedFood.getType() != null) {
            existingFood.setType(updatedFood.getType());
        }
        if (updatedFood.getCompound() != null) {
            existingFood.setCompound(updatedFood.getCompound());
        }
        if (updatedFood.getCalories() != 0) {
            existingFood.setCalories(updatedFood.getCalories());
        }
        if (updatedFood.getPrice() != 0.0) {
            existingFood.setPrice(updatedFood.getPrice());
        }
        if (updatedFood.getDescription() != null) {
            existingFood.setDescription(updatedFood.getDescription());
        }
        if (updatedFood.isVegetarian() != existingFood.isVegetarian()) {
            existingFood.setVegetarian(updatedFood.isVegetarian());
        }
        Food savedFood = foodRepository.save(existingFood);

        return ResponseEntity.ok(savedFood);
    }
}
