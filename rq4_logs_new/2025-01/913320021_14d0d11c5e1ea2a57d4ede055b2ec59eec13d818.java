package com.management.restaurant.controllers;

import com.management.restaurant.DTO.client.ClientResponseDTO;
import com.management.restaurant.DTO.ordens.DishDTO;
import com.management.restaurant.DTO.restaurant.DishRequestDTO;
import com.management.restaurant.DTO.restaurant.DishResponseDTO;
import com.management.restaurant.models.client.Client;
import com.management.restaurant.models.restaurant.Dish;
import com.management.restaurant.services.DishService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DishControllerTest {

  private final WebTestClient webTestClient;
  private DishService dishService;
  private Dish existingDish;
  private Dish updatedDish;
  private DishDTO dishDTO;

  public DishControllerTest(){
    dishService = mock(DishService.class);
    webTestClient = WebTestClient.bindToController(new DishController(dishService)).build();
  }

  @BeforeEach
  void setup() {
    existingDish = new Dish(1L, "Pasta", null, 12.99, false);
    updatedDish = new Dish(1L, "Pasta Updated", null, 15.99, true);
    dishDTO = new DishDTO();
    dishDTO.setName("Pasta Updated");
    dishDTO.setPrice(15.99);
    dishDTO.setPopular(true);

  }

  @Test
  @DisplayName("obtener todos los platos")
  void getAllDishes() {

    List<Dish> dishList = List.of(
      new Dish(1L, "Pasta", null, 12.99, false),
      new Dish(2L, "Pizza", null, 15.99, true)
    );
    when(dishService.getAllDish()).thenReturn(dishList);

    webTestClient.get()
      .uri("/api/dish")
      .exchange()
      .expectStatus().isOk()
      .expectHeader().contentType(MediaType.APPLICATION_JSON)
      .expectBodyList(DishDTO.class)
      .hasSize(2)
      .value(dish -> {
        assertEquals(1L, dish.get(0).getId());
        assertEquals("Pasta", dish.get(0).getName());
        assertEquals(12.99, dish.get(0).getPrice());
        assertFalse(dish.get(0).getPopular());

        assertEquals(2L, dish.get(1).getId());
        assertEquals("Pizza", dish.get(1).getName());
        assertEquals(15.99, dish.get(1).getPrice());
        assertTrue(dish.get(1).getPopular());

      });
    Mockito.verify(dishService).getAllDish();
  }

  @Test
  @DisplayName("Crear plato nuevo")
  void createDish() {
    Dish dish = new Dish(1L, "Pasta", null, 12.99, false);

    when(dishService.createDish(any(DishRequestDTO.class))).thenReturn(dish);

    webTestClient
      .post()
      .uri("/api/dish")
      .bodyValue(dishDTO)
      .exchange()
      .expectStatus().isOk()
      .expectHeader().contentType(MediaType.APPLICATION_JSON)
      .expectBodyList(DishDTO.class)
      .value(dishes -> {
        assertEquals(1L, dishes.get(0).getId());
        assertEquals("Pasta", dishes.get(0).getName());
        assertEquals(12.99, dishes.get(0).getPrice());
        assertFalse(dishes.get(0).getPopular());

      });
    Mockito.verify(dishService).createDish(any(DishRequestDTO.class));
  }

  @Test
  @DisplayName("Actualizar plato")
  void updateDish() {
    DishRequestDTO dishRequestDTO = new DishRequestDTO();
    dishRequestDTO.setName("Pasta");
    dishRequestDTO.setPrice(15.99);
    dishRequestDTO.setPopular(true);
    dishRequestDTO.setMenuRestaurantId(1L);

    when(dishService.updateDish(eq(existingDish.getId()), any(DishRequestDTO.class))).thenReturn(updatedDish);

    webTestClient.put()
        .uri("/api/dish/{id}", existingDish.getId())
        .bodyValue(dishRequestDTO)
        .exchange()
        .expectStatus().isOk()
        .expectHeader().contentType(MediaType.APPLICATION_JSON)
        .expectBody(DishResponseDTO.class)
        .value(response -> {
          assertEquals(updatedDish.getId(), response.getId());
          assertEquals(updatedDish.getName(), response.getName());
          assertEquals(updatedDish.getPrice(), response.getPrice());
          assertTrue(updatedDish.getPopular());
        });
      Mockito.verify(dishService).updateDish(eq(existingDish.getId()), any(DishRequestDTO.class));
    }


  @Test
  @DisplayName("Eliminar plato")
  void deleteDish() {

    doNothing().when(dishService).deleteDish(existingDish.getId());

    webTestClient.delete()
      .uri("/api/dish/{id}", existingDish.getId())
      .exchange()
      .expectStatus().isOk();

    Mockito.verify(dishService).deleteDish(existingDish.getId());
  }

}