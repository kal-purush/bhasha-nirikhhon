package com.novi.eindopdracht.recipeDiary.recipeDiary.service;

import com.novi.eindopdracht.recipeDiary.recipeDiary.dto.PhotoDto;
import com.novi.eindopdracht.recipeDiary.recipeDiary.dto.RecipeDto;
import com.novi.eindopdracht.recipeDiary.recipeDiary.exception.ResourceNotFoundException;
import com.novi.eindopdracht.recipeDiary.recipeDiary.model.Photo;
import com.novi.eindopdracht.recipeDiary.recipeDiary.model.Recipe;
import com.novi.eindopdracht.recipeDiary.recipeDiary.repository.PhotoRepository;
import com.novi.eindopdracht.recipeDiary.recipeDiary.repository.RecipeRepository;
import org.springframework.stereotype.Service;

import java.util.Base64;

@Service
public class RecipePhotoService {

    private final RecipeRepository rRepos;
    private final PhotoRepository pRepos;


    public RecipePhotoService(RecipeRepository rRepos, PhotoRepository pRepos) {
        this.rRepos = rRepos;
        this.pRepos = pRepos;
    }

    public PhotoDto getPhoto(Long recipeId){
        Recipe recipe = rRepos.findById(recipeId)
                .orElseThrow(() -> new ResourceNotFoundException("Recipe", "id", recipeId));

        Photo photo = recipe.getPhoto();

        PhotoDto photoDto = new PhotoDto();
        photoDto.setPhotoId(photo.getPhotoId());
        photoDto.setDishName(photo.getDishName());
        photoDto.setImageData(Base64.getEncoder().encodeToString(photo.getImageData()));

        return photoDto;

    }

    public RecipeDto linkPhotoToRecipe(Long recipeId, Long photoId){

        Recipe recipe = rRepos.findById(recipeId)
                .orElseThrow(() -> new ResourceNotFoundException("Recipe", "id", recipeId));

        Photo photo = pRepos.findById(photoId)
                .orElseThrow(() -> new ResourceNotFoundException("Photo", "id", photoId));

        recipe.setPhoto(photo);

        Recipe recipe1 = rRepos.save(recipe);

        return recipePhotoModelToDto(recipe1);

    }

    public RecipeDto recipePhotoModelToDto(Recipe recipe){

        RecipeDto recipeDto = new RecipeDto();
        recipeDto.setRecipeId(recipe.getRecipeId());

        return recipeDto;

    }

}