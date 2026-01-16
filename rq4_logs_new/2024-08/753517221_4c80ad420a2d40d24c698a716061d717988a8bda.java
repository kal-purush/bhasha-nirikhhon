package com.itcook.cooking.api.domains.post.service.dto;

import com.itcook.cooking.domain.domains.post.domain.entity.dto.RecipeAddDto;
import com.itcook.cooking.domain.domains.post.domain.entity.dto.RecipeUpdateDto;
import com.itcook.cooking.domain.domains.post.domain.enums.CookingType;
import com.itcook.cooking.domain.domains.user.domain.enums.LifeType;
import lombok.Builder;

import java.util.List;

import static com.itcook.cooking.domain.domains.post.domain.entity.dto.RecipeUpdateDto.*;

@Builder
public record RecipeUpdateServiceDto(
        Long recipeId,
        String email,
        String recipeName,
        Integer recipeTime,
        String introduction,
        String mainFileExtension,
        List<String> foodIngredients,
        List<CookingType> cookingType,
        List<LifeType> lifeType,
        List<RecipeProcessUpdateServiceDto> recipeProcess
) {
    @Builder
    public record RecipeProcessUpdateServiceDto(
            Integer stepNum,
            String recipeWriting,
            String fileExtension
    ) {
    }

    public RecipeUpdateDto toDomainDto(Long userId) {
        return RecipeUpdateDto.builder()
                .recipeId(recipeId)
                .userId(userId)
                .recipeName(recipeName)
                .recipeTime(recipeTime)
                .introduction(introduction)
                .mainFileExtension(mainFileExtension)
                .foodIngredients(foodIngredients)
                .cookingType(cookingType)
                .lifeTypes(lifeType)
                .recipeProcess(toServiceDto())
                .build();
    }

    private List<RecipeProcessUpdateDto> toServiceDto() {
        return recipeProcess.stream()
                .map(rp -> RecipeProcessUpdateDto.builder()
                        .stepNum(rp.stepNum)
                        .recipeWriting(rp.recipeWriting)
                        .fileExtension(rp.fileExtension)
                        .build()).toList();
    }
}