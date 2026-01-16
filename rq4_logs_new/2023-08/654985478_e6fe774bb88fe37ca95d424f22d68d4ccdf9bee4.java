package choikang.MealGuard.recipe.controller;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import choikang.MealGuard.recipe.dto.RecipeDto;
import choikang.MealGuard.recipe.entity.Recipe;
import choikang.MealGuard.recipe.mapper.RecipeMapper;
import choikang.MealGuard.recipe.service.RecipeService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import java.util.ArrayList;
import java.util.List;


@ExtendWith(SpringExtension.class)
@WebMvcTest(RecipeController.class)
@AutoConfigureMockMvc(addFilters = false)
public class RecipeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private RecipeService recipeService;

    @MockBean
    private RecipeMapper mapper;

    @DisplayName("레시피 목록 불러오기")
    @Test
    public void testGetRecipes() throws Exception {

        List<Recipe> mockRecipes = new ArrayList<>();

        Page<Recipe> mockRecipePage = new PageImpl<>(mockRecipes);

        when(recipeService.findRecipes(anyString(), anyString(), anyInt(), anyInt())).thenReturn(mockRecipePage);

        List<RecipeDto.ListResponse> mockListResponses = new ArrayList<>();

        when(mapper.recipeToListResponse(anyList())).thenReturn(mockListResponses);

        mockMvc.perform(MockMvcRequestBuilders.get("/recipes")
                        .param("name", "닭가슴살")
                        .param("sortBy", "highProtein")
                        .param("page", "1")
                        .param("size", "10")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.data").isArray());

        verify(recipeService).findRecipes(eq("닭가슴살"), eq("highProtein"), eq(1), eq(10));
        verify(mapper).recipeToListResponse(eq(mockRecipes));
    }
}