package com.example.a327finalprojectcalorietracker;

import java.util.List;

import java.util.ArrayList;
import java.util.List;

public class UserFoods {
    private List<foodItem> foodItems;
    private labelNutrients totalNutrients;

    public UserFoods() {
        this.foodItems = new ArrayList<>();
        this.totalNutrients = new labelNutrients();
    }

    public void addFood(foodItem food) {
        this.foodItems.add(food);
        this.totalNutrients.setFat(this.totalNutrients.getFat() + food.getLabelNutrients().getFat());
        this.totalNutrients.setSaturatedFat(this.totalNutrients.getSaturatedFat() + food.getLabelNutrients().getSaturatedFat());
        this.totalNutrients.setTransFat(this.totalNutrients.getTransFat() + food.getLabelNutrients().getTransFat());
        this.totalNutrients.setCholesterol(this.totalNutrients.getCholesterol() + food.getLabelNutrients().getCholesterol());
        this.totalNutrients.setSodium(this.totalNutrients.getSodium() + food.getLabelNutrients().getSodium());
        this.totalNutrients.setCarbohydrates(this.totalNutrients.getCarbohydrates() + food.getLabelNutrients().getCarbohydrates());
        this.totalNutrients.setFiber(this.totalNutrients.getFiber() + food.getLabelNutrients().getFiber());
        this.totalNutrients.setSugars(this.totalNutrients.getSugars() + food.getLabelNutrients().getSugars());
        this.totalNutrients.setProtein(this.totalNutrients.getProtein() + food.getLabelNutrients().getProtein());
        this.totalNutrients.setCalcium(this.totalNutrients.getCalcium() + food.getLabelNutrients().getCalcium());
        this.totalNutrients.setIron(this.totalNutrients.getIron() + food.getLabelNutrients().getIron());
        this.totalNutrients.setPotassium(this.totalNutrients.getPotassium() + food.getLabelNutrients().getPotassium());
        this.totalNutrients.setCalories(this.totalNutrients.getCalories() + food.getLabelNutrients().getCalories());
    }

    public void removeFood(foodItem food) {
        if (this.foodItems.remove(food)) {
            this.totalNutrients.setFat(this.totalNutrients.getFat() - food.getLabelNutrients().getFat());
            this.totalNutrients.setSaturatedFat(this.totalNutrients.getSaturatedFat() - food.getLabelNutrients().getSaturatedFat());
            this.totalNutrients.setTransFat(this.totalNutrients.getTransFat() - food.getLabelNutrients().getTransFat());
            this.totalNutrients.setCholesterol(this.totalNutrients.getCholesterol() - food.getLabelNutrients().getCholesterol());
            this.totalNutrients.setSodium(this.totalNutrients.getSodium() - food.getLabelNutrients().getSodium());
            this.totalNutrients.setCarbohydrates(this.totalNutrients.getCarbohydrates() - food.getLabelNutrients().getCarbohydrates());
            this.totalNutrients.setFiber(this.totalNutrients.getFiber() - food.getLabelNutrients().getFiber());
            this.totalNutrients.setSugars(this.totalNutrients.getSugars() - food.getLabelNutrients().getSugars());
            this.totalNutrients.setProtein(this.totalNutrients.getProtein() - food.getLabelNutrients().getProtein());
            this.totalNutrients.setCalcium(this.totalNutrients.getCalcium() - food.getLabelNutrients().getCalcium());
            this.totalNutrients.setIron(this.totalNutrients.getIron() - food.getLabelNutrients().getIron());
            this.totalNutrients.setPotassium(this.totalNutrients.getPotassium() - food.getLabelNutrients().getPotassium());
            this.totalNutrients.setCalories(this.totalNutrients.getCalories() - food.getLabelNutrients().getCalories());
        }
    }


    public List<foodItem> getFoodItems() {
        return foodItems;
    }

    public labelNutrients getTotalNutrients() {
        return totalNutrients;
    }
}