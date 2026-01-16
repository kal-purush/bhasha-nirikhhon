package com.management.restaurant.controllers;

import com.management.restaurant.DTO.restaurant.DishRequestDTO;
import com.management.restaurant.DTO.restaurant.MenuResponseDTO;
import com.management.restaurant.DTO.restaurant.MenuResquetDTO;
import com.management.restaurant.models.restaurant.Dish;
import com.management.restaurant.models.restaurant.MenuRestaurant;
import com.management.restaurant.models.restaurant.Restaurant;
import com.management.restaurant.repositories.DishRepository;
import com.management.restaurant.repositories.MenuRepository;
import com.management.restaurant.services.DishService;
import com.management.restaurant.services.MenuService;
import com.management.restaurant.services.observer.ObserverManager;
import com.management.restaurant.utils.MenuDtoConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.LocalTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MenuControllerTest {

  private WebTestClient webTestClient;
  private MenuService menuService;
  private MenuRepository menuRepository;
  private DishRepository dishRepository;
  private MenuRestaurant menuRestaurant;
  private ObserverManager<Dish> observerManager;
  private DishRequestDTO dishRequestDTO;

  @BeforeEach
  void setUp() {
    menuService = mock(MenuService.class);
    dishRepository = mock(DishRepository.class);
    menuRepository = mock(MenuRepository.class);
    observerManager = mock(ObserverManager.class);
    webTestClient = WebTestClient.bindToController(new MenuController(menuService)).build();


    Restaurant restaurant = new Restaurant(1L, "Restaurante Test", "123 Main St", "555-1234", LocalTime.of(9, 0), LocalTime.of(17, 0), null);
    menuRestaurant = new MenuRestaurant(1L, "Menu 1", restaurant, List.of());
    dishRequestDTO = new DishRequestDTO();
    dishRequestDTO.setName("Pasta");
    dishRequestDTO.setPrice(15.99);
    dishRequestDTO.setPopular(false);
    dishRequestDTO.setMenuRestaurantId(menuRestaurant.getIdMenu());
  }

  @Test
  @DisplayName("Crear un menu")
  void createMenu() {
      MenuResquetDTO menuRequestDTO = new MenuResquetDTO();
      menuRequestDTO.setDescription("Menu 1");
      menuRequestDTO.setRestaurantId(1L);
      menuRequestDTO.setDishes(List.of());

      Restaurant restaurant = new Restaurant(1L, "Restaurante Test", "123 Main St", "555-1234", LocalTime.of(9, 0), LocalTime.of(22, 0), null);
      MenuRestaurant menu = new MenuRestaurant(1L, "Menu 1", restaurant, List.of());

      MenuResponseDTO expectedResponseDTO = MenuDtoConverter.convertToResponseDTO(menu);

      when(menuService.addMenu(any(MenuResquetDTO.class))).thenReturn(menu);

    webTestClient.post()
      .uri("/api/menu")
      .bodyValue(menuRequestDTO)
      .exchange()
      .expectStatus().isOk()
      .expectHeader().contentType(MediaType.APPLICATION_JSON)
      .expectBody(MenuResponseDTO.class)
      .value(response ->{
        assertEquals(expectedResponseDTO.getId(), response.getId());
        assertEquals(expectedResponseDTO.getDescription(), response.getDescription());
        assertEquals(expectedResponseDTO.getDishes().size(), response.getDishes().size());
      });
    verify(menuService).addMenu(any(MenuResquetDTO.class));
  }

  @Test
  @DisplayName("Actualizar un menu")
  void updateMenu() {
    MenuResquetDTO menuRequestDTO = new MenuResquetDTO();
    menuRequestDTO.setDescription("Menu Updated");
    menuRequestDTO.setRestaurantId(1L);
    menuRequestDTO.setDishes(List.of());
    MenuRestaurant updatedMenu = new MenuRestaurant(1L, "Menu Updated", new Restaurant(), List.of());
    MenuResponseDTO updatedResponseDTO = MenuDtoConverter.convertToResponseDTO(updatedMenu);
    when(menuService.updateMenu(any(MenuResquetDTO.class))).thenReturn(updatedMenu);

    webTestClient.put()
      .uri("/api/menu")
      .contentType(MediaType.APPLICATION_JSON)
      .bodyValue(menuRequestDTO)
      .exchange()
      .expectStatus().isOk()
      .expectHeader()
      .contentType(MediaType.APPLICATION_JSON)
      .expectBody(MenuResponseDTO.class)
      .value(response -> {
        assertEquals(updatedResponseDTO.getId(), response.getId());
        assertEquals(updatedResponseDTO.getDescription(), response.getDescription());
        assertEquals(updatedResponseDTO.getDishes().size(), response.getDishes().size());
      });

    verify(menuService).updateMenu(any(MenuResquetDTO.class));
  }

  @Test
  @DisplayName("Eliminar un menu")
  void deleteMenu() {
    Long restaurantId = 1L;
    webTestClient.delete()
      .uri("/api/menu/{restaurantId}", restaurantId)
      .exchange()
      .expectStatus().isNoContent();
    verify(menuService).deleteMenu(restaurantId); }
}