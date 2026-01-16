package com.itcook.cooking.api.domains.post.dto.recipe;

import com.itcook.cooking.domain.domains.post.domain.entity.RecipeProcess;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Comparator;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@Schema(name = "recipe process response")
public class RecipeProcessGetResponse {

    private Integer stepNum;
    private String recipeWriting;
    private String recipeProcessImagePath;

    public static List<RecipeProcessGetResponse> fromDto(List<RecipeProcess> recipeProcessList) {
        return recipeProcessList.stream()
                .map(rp -> RecipeProcessGetResponse
                        .of(
                                rp.getStepNum(),
                                rp.getRecipeWriting(),
                                rp.getRecipeProcessImagePath()))
                .sorted(Comparator.comparing(RecipeProcessGetResponse::getStepNum)).toList();
    }

    public static RecipeProcessGetResponse of(Integer stepNum, String recipeWriting, String recipeProcessImagePath) {
        return RecipeProcessGetResponse.builder()
                .stepNum(stepNum)
                .recipeWriting(recipeWriting)
                .recipeProcessImagePath(recipeProcessImagePath)
                .build();
    }
}