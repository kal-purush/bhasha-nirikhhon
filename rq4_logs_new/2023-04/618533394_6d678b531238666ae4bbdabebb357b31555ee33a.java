package com.novi.eindopdracht.recipeDiary.recipeDiary.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.novi.eindopdracht.recipeDiary.recipeDiary.model.RecipeDiary;

public class RecipeDiaryOverviewDto {

    public Long recipeId;
    public String name;
    public int rating;
    public String categoryName;

    @JsonIgnore
    public RecipeDiary recipeDiary;

    public void setRecipeId(Long recipeId) {
        this.recipeId = recipeId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public RecipeDiary getRecipeDiary() {
        return recipeDiary;
    }

    public void setRecipeDiary(RecipeDiary recipeDiary) {
        this.recipeDiary = recipeDiary;
    }
}