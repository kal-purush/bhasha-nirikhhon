package com.management.restaurant.utils;

import com.management.restaurant.DTO.restaurant.DishRequestDTO;
import com.management.restaurant.models.restaurant.Dish;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DishDtoConverterTest {

  @Test
  void convertToEntity() {
    DishRequestDTO dishRequestDTO = new DishRequestDTO();
    dishRequestDTO.setName("Sushi");
    dishRequestDTO.setPrice(15.99);
    dishRequestDTO.setPopular(true);

    Dish dish = DishDtoConverter.convertToEntity(dishRequestDTO);

    assertNotNull(dish);
    assertEquals("Sushi", dish.getName());
    assertEquals(15.99, dish.getPrice());
    assertTrue(dish.getPopular());
  }
}