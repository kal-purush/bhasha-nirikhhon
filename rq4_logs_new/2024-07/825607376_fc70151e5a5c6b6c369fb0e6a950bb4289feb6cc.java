package refooding.api.domain.recipe.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;

@Getter
public class RecipeData {
    @JsonProperty("recipes")
    private List<Recipe> recipes;

    @Getter
    public static class Recipe {
        @JsonProperty("RCP_NM")
        private String name;
        @JsonProperty("RCP_PARTS_DTLS")
        private String ingredientsDetails;
        @JsonProperty("HASH_TAG")
        private String hashTag;
        @JsonProperty("RCP_NA_TIP")
        private String tip;
        @JsonProperty("ATT_FILE_NO_MAIN")
        private String mainImage;
        @JsonProperty("manuals")
        private List<Manual> manuals; // 메뉴얼 개수는 레시피마다 달라질 수 있으므로 리스트로 저장

    }

    @Getter
    public static class Manual {
        @JsonProperty("content")
        private String content;
        @JsonProperty("imageSrc")
        private String imageSrc;

    }

}


