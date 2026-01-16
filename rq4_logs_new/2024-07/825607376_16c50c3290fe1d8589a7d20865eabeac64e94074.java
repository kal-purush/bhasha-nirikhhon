package refooding.api.domain.recipe.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public class FavoriteRecipeToggleResponse {
    private boolean success;
    private String message;
}