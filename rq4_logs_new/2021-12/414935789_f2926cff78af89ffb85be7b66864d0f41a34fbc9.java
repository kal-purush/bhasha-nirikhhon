package com.kitchen.iChef.Controller.Model.Request;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.List;

@Getter
@Setter
@ToString
public class UpdateRecipeRequest {
    @NotBlank(message = "The title of the recipe is not valid!")
    private String title;

    @NotBlank(message = "The steps of the recipe is not valid!")
    private String steps;

    @Min(value = 1, message = "The difficulty of the recipe is not valid!")
    private Float difficulty;

    @Min(value = 1, message = "The preparationTime of the recipe is not valid!")
    private Integer preparationTime;

    @Min(value = 1, message = "The portions of the recipe is not valid!")
    private Integer portions;

    @NotBlank(message = "The notes of the recipe is not valid!")
    private String notes;

    @NotBlank(message = "The image path of the recipe is not valid!")
    private String imagePath;

    @NotEmpty(message = "The ingredients of the recipe is not valid!")
    private List<@Valid RecipeIngredientRequest> recipeIngredientList;

    @NotEmpty(message = "The utensils of the recipe is not valid!")
    private List<@Valid RecipeUtensilRequest> recipeUtensilList;
}