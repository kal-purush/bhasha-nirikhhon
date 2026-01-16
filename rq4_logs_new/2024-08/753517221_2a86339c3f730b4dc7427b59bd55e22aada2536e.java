package com.itcook.cooking.api.domains.post.service.dto;

import com.itcook.cooking.domain.domains.post.domain.entity.dto.RecipeAddDto;
import com.itcook.cooking.domain.domains.post.domain.enums.CookingType;
import com.itcook.cooking.domain.domains.user.domain.enums.LifeType;
import lombok.Builder;

import java.util.List;

import static com.itcook.cooking.domain.domains.post.domain.entity.dto.RecipeAddDto.*;

@Builder
public record RecipeAddServiceDto(
        String email,
        String recipeName,
        Integer recipeTime,
        String introduction,
        String mainFileExtension,
        List<String> foodIngredients,
        List<CookingType> cookingType,
        List<LifeType> lifeType,
        List<RecipeProcessAddServiceDto> recipeProcess
) {
    @Builder
    public record RecipeProcessAddServiceDto(
            Integer stepNum,
            String recipeWriting,
            String fileExtension
    ) {
    }

    public RecipeAddDto toDomainDto(Long userId) {
        return RecipeAddDto.builder()
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

    private List<RecipeProcessAddDto> toServiceDto() {
        return recipeProcess.stream()
                .map(rp -> RecipeProcessAddDto.builder()
                        .stepNum(rp.stepNum)
                        .recipeWriting(rp.recipeWriting)
                        .fileExtension(rp.fileExtension)
                        .build()).toList();
    }

}