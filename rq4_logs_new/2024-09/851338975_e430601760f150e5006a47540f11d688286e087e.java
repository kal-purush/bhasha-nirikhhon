package com.reservibe.application.controller.restaurant;

import com.reservibe.domain.input.restaurant.RestaurantInput;
import com.reservibe.domain.usecase.restaurant.register.RegisterRestaurantUseCase;
import com.reservibe.helper.RestaurantHelper;
import com.reservibe.infra.model.restaurant.RestaurantModel;
import com.reservibe.infra.repository.restaurant.RestaurantRepository;
import com.reservibe.infra.repository.table.TableModelRepository;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MockMvcBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.SerializationFeature;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RestaurantRegisterController.class)
class RestaurantRegisterControllerTest {

    private MockMvc mockMvc;

    @Mock
    private RestaurantRepository restaurantRepository;

    @Mock
    private TableModelRepository tableModelRepository;

    @InjectMocks
    private RestaurantRegisterController controller;

    @Mock
    RegisterRestaurantUseCase registerRestaurantUseCase;


    RestaurantHelper helper = new RestaurantHelper();

    AutoCloseable mock;

    @BeforeEach
    void setUp() {
        //RestAssured.port = port;
        mock = MockitoAnnotations.openMocks(this);
        controller = new RestaurantRegisterController(restaurantRepository, tableModelRepository);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        mock.close();
    }

    @Test
    void shouldRegisterRestaurantPost() throws Exception {
        var id = UUID.randomUUID();
        var restaurant = helper.createRestaurant(id);
        var restaurantInput = helper.createRestaurantInput();

        doNothing().when(registerRestaurantUseCase).execute(restaurantInput);

        mockMvc.perform(MockMvcRequestBuilders.post("/restaurant/register")
                        //.contentType("application/json")
                        //.content(new ObjectMapper().writeValueAsString(restaurantInput))
                )
                .andExpect(status().isCreated()); // Adjust status if expected to be different




    }


}