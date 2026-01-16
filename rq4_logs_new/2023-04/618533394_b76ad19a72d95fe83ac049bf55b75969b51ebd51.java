package com.novi.eindopdracht.recipeDiary.recipeDiary.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.novi.eindopdracht.recipeDiary.recipeDiary.dto.IngredientDto;
import com.novi.eindopdracht.recipeDiary.recipeDiary.security.JwtService;
import com.novi.eindopdracht.recipeDiary.recipeDiary.service.IngredientService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(IngredientController.class)
@ActiveProfiles("test")
class IngredientControllerTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    JwtService jwtService;

    @MockBean
    IngredientService iService;

    @Test
    @WithMockUser(username = "testuser", roles = "USER")
    void shouldReturnIngredient() throws Exception {

        Long ingredientId = 1L;
        IngredientDto idto = new IngredientDto();
        idto.ingredientId = ingredientId;
        idto.ingredientName = "Test ingredient";
        idto.quantity = 100;
        idto.unit = "g";

        when(iService.getIngredient(ingredientId)).thenReturn(idto);

        ResultActions resultActions = mockMvc.perform(MockMvcRequestBuilders.get("/ingredients/" + ingredientId));

        resultActions.andExpect(status().isOk())
                .andExpect(jsonPath("$.ingredientId").value(idto.ingredientId))
                .andExpect(jsonPath("$.ingredientName").value(idto.ingredientName))
                .andExpect(jsonPath("$.quantity").value(idto.quantity))
                .andExpect(jsonPath("$.unit").value(idto.unit));
    }
}